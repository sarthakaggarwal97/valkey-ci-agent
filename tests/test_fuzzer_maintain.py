"""Tests for the walk's maintenance: what Claude may do through actions.json, the pushes, and the workflow's guards."""
from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

import scripts.fuzzer.maintain as maintain
from scripts.ai.claude_code import DEFAULT_CLAUDE_ENV_ALLOWLIST
from scripts.ai.runtime import AGENT_PROFILES, AgentRunResult
from scripts.fuzzer.maintain import FUZZER_REPO, LOG_TITLE, apply, export_patch, guard, push, validate

TITLES = {1: "[fuzzer-walk] crash in t_list.c", 2: "Something else", 3: LOG_TITLE}


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True, text=True).stdout.strip()


def _repo(path: Path) -> Path:
    path.mkdir(parents=True)
    _git(path, "init", "-q", "-b", "main")
    _git(path, "config", "user.email", "t@example.com")
    _git(path, "config", "user.name", "t")
    _commit(path, "README", "base\n", "Base")
    return path


def _commit(repo: Path, name: str, text: str, message: str) -> str:
    file = repo / name
    file.parent.mkdir(parents=True, exist_ok=True)
    file.write_text(text)
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-s", "-m", message)
    return _git(repo, "rev-parse", "HEAD")


def _validate(doc):
    return validate(doc, TITLES.get)


# --- actions.json --------------------------------------------------------------


def test_a_vetted_comment_and_close_on_walk_issues_are_accepted():
    comments, closes, report, refused = _validate({
        "comments": [{"issue": 1, "body": "Vetted: real\n\nrepro"}, {"issue": 3, "body": "Vetted: a note"}],
        "close": [{"issue": 1, "reason": "not planned", "body": "Vetted: false positive"}],
        "report": "Vetted #1.",
    })
    assert [c["issue"] for c in comments] == [1, 3] and [c["issue"] for c in closes] == [1]
    assert (report, refused) == ("Vetted #1.", [])


def test_an_action_naming_a_valkey_io_repository_is_refused():
    comments, _, _, refused = _validate({"comments": [{"issue": 1, "repo": "valkey-io/valkey", "body": "Vetted: x"}]})
    assert comments == [] and refused == ["comments on #1: not an object of body, issue"]


def test_an_action_on_an_issue_the_walk_did_not_file_is_refused():
    comments, closes, _, refused = _validate({
        "comments": [{"issue": 2, "body": "Vetted: x"}, {"issue": 9, "body": "Vetted: x"}],
        # The log issue takes the report and comments, but is never closed.
        "close": [{"issue": 3, "reason": "completed", "body": "Vetted: x"}],
    })
    assert comments == [] and closes == []
    assert refused == ["comments on #2: not a [fuzzer-walk] issue", "comments on #9: no such issue",
                       "close on #3: not a [fuzzer-walk] issue"]


def test_a_body_that_does_not_start_with_vetted_is_refused():
    comments, closes, _, refused = _validate({
        "comments": [{"issue": 1, "body": "Looks real. Vetted: real"}],
        "close": [{"issue": 1, "reason": "not planned", "body": ""}],
    })
    assert comments == [] and closes == []
    assert refused == ["comments on #1: the body does not start with 'Vetted:'",
                       "close on #1: the body does not start with 'Vetted:'"]


def test_close_reasons_issue_numbers_and_lengths_are_checked():
    _, closes, _, refused = _validate({
        "comments": [{"issue": True, "body": "Vetted: x"}, {"issue": 1, "body": "Vetted: " + "x" * 20000}],
        "close": [{"issue": 1, "reason": "duplicate", "body": "Vetted: x"}],
    })
    assert closes == []
    assert refused == ["comments on #?: not an issue number",
                       "comments on #1: the body is longer than 20000 characters",
                       "close on #1: the reason is not one of not planned, completed"]


def test_anything_but_comments_close_and_report_refuses_the_whole_file():
    for doc in ([], {"comments": [], "push": "valkey-io/valkey"}):
        assert _validate(doc) == ([], [], "", ["actions.json is not an object of comments, close and report; "
                                               "all of it was refused"])


def test_a_long_report_is_cut_and_too_many_actions_are_refused():
    many = [{"issue": 1, "body": "Vetted: x"}] * 51
    comments, _, report, refused = _validate({"comments": many, "report": "\n".join(f"l{i}" for i in range(30))})
    assert len(comments) == 50 and report.splitlines()[-1] == "l24"
    assert refused == ["the report was cut to 25 lines and 5000 characters", "comments: the 1 past the first 50"]


def test_nothing_is_written_to_valkey_io():
    for repo in ("valkey-io/valkey-fuzzer", "Valkey-IO/valkey"):
        with pytest.raises(ValueError, match="never to valkey-io"):
            guard(repo)
    with pytest.raises(ValueError):
        apply(SimpleNamespace(), "valkey-io/valkey-fuzzer", {}, [], "")
    with pytest.raises(ValueError, match="only sarthakaggarwal97/valkey-fuzzer"):
        apply(SimpleNamespace(), "someone/valkey-fuzzer", {}, [], "")
    with pytest.raises(SystemExit):
        maintain.main(["push", "--name", "valkey-fuzzer", "--repo", "valkey-io/valkey-fuzzer", "--branch", "b",
                       "--base", "a" * 40, "--patch", "p", "--work", "w", "--notes", "n"])


class _Issue:
    def __init__(self, number, title, state="open"):
        self.number, self.title, self.state, self.pull_request = number, title, state, None
        self.comments, self.edits = [], []

    def create_comment(self, body):
        self.comments.append(body)

    def edit(self, **kw):
        self.edits.append(kw)


class _Repo:
    def __init__(self, issues):
        self.issues, self.created = {i.number: i for i in issues}, []

    def get_issue(self, number):
        from github import UnknownObjectException

        if number not in self.issues:
            raise UnknownObjectException(404, {}, {})
        return self.issues[number]

    def get_issues(self, state):
        return [i for i in self.issues.values() if i.state == state]

    def create_issue(self, title, body):
        issue = self.issues[100] = _Issue(100, title)
        self.created.append(title)
        return issue


def test_apply_comments_closes_and_posts_the_report_on_a_new_log_issue():
    walk, other = _Issue(1, TITLES[1]), _Issue(2, TITLES[2])
    repo = _Repo([walk, other])
    gh = SimpleNamespace(get_repo=lambda name: repo if name == FUZZER_REPO else None)
    doc = {"comments": [{"issue": 2, "body": "Vetted: x"}],
           "close": [{"issue": 1, "reason": "not planned", "body": "Vetted: false positive"}], "report": "Closed #1."}
    text = apply(gh, FUZZER_REPO, doc, ["valkey-ci-agent: pushed 1 commit(s) to fuzzer-replay"], "https://run")
    assert walk.comments == ["Vetted: false positive"] and walk.edits == [{"state": "closed",
                                                                         "state_reason": "not_planned"}]
    assert other.comments == [] and repo.created == [LOG_TITLE]
    assert repo.issues[100].comments == [text]
    assert text == ("Closed #1.\n\n- valkey-ci-agent: pushed 1 commit(s) to fuzzer-replay\n"
                    "- refused: comments on #2: not a [fuzzer-walk] issue\n\n[Run](https://run)")


# --- Claude and the pushes -----------------------------------------------------


def test_the_profile_gives_claude_a_shell_but_no_github_token():
    profile = AGENT_PROFILES["fuzzer_walk_maintain"]
    assert profile.allowed_tools == "Read,Write,Edit,Glob,Grep,Bash"
    assert profile.env_allowlist == DEFAULT_CLAUDE_ENV_ALLOWLIST
    assert not [name for name in profile.env_allowlist if "GITHUB" in name or name.startswith(("GH_", "ACTIONS_"))]
    # The role's credentials last an hour, renewed only before a run.
    assert profile.timeout < 3600


def _work(tmp_path: Path) -> Path:
    work = tmp_path / "work"
    for name in maintain.CHECKOUTS:
        _repo(work / name)
    return work


def _answer() -> AgentRunResult:
    return AgentRunResult("fuzzer_walk_maintain", "", "", 0, "", "", "", "", "", "")


def test_run_exports_each_checkouts_new_commits_as_a_patch(tmp_path):
    work = _work(tmp_path)
    seen = {}

    def runner(prompt, cwd):
        seen["prompt"] = prompt
        _commit(cwd / "valkey-ci-agent", "scripts/fuzzer/walk.py", "fixed\n", "Fix the walk")
        (tmp_path / "out" / "actions.json").write_text(json.dumps({"report": "Fixed."}))
        return _answer()

    bases, patched, code = maintain.run(work, tmp_path / "in", tmp_path / "out", runner=runner)
    assert patched == {"valkey-ci-agent": True, "valkey-fuzzer": False} and code == 0
    patch = (tmp_path / "out" / "valkey-ci-agent.patch").read_text()
    assert "Subject: [PATCH] Fix the walk" in patch and "From: Sarthak Aggarwal <sarthagg@amazon.com>" in patch
    assert "Signed-off-by: Sarthak Aggarwal <sarthagg@amazon.com>" in patch
    assert bases["valkey-fuzzer"] == _git(work / "valkey-fuzzer", "rev-parse", "HEAD")
    prompt = seen["prompt"]
    assert f"{tmp_path / 'out'}/actions.json" in prompt and "untrusted data" in prompt
    assert "VALKEY_FUZZER_DIR=" in prompt and "-m 'not live and not slow'" in prompt and "ruff format --check" in prompt


def test_commits_that_do_not_descend_from_the_base_are_refused(tmp_path):
    repo = _repo(tmp_path / "r")
    base = _commit(repo, "a", "1\n", "One")
    _git(repo, "checkout", "-q", "--orphan", "other")
    _commit(repo, "b", "2\n", "Unrelated")
    assert export_patch(repo, base, tmp_path / "p.patch") is False and not (tmp_path / "p.patch").exists()


def _patched(tmp_path: Path, path: str) -> tuple[Path, str, Path]:
    """A bare remote whose main is at a base, and a patch of one commit on it changing ``path``."""
    clone = _repo(tmp_path / "clone")
    base = _git(clone, "rev-parse", "HEAD")
    remote = tmp_path / "remote.git"
    subprocess.run(["git", "clone", "-q", "--bare", str(clone), str(remote)], check=True)
    _commit(clone, path, "x\n", "Change it")
    assert export_patch(clone, base, tmp_path / "c.patch")
    return remote, base, tmp_path / "c.patch"


def test_a_verified_patch_is_pushed_onto_its_branch(tmp_path):
    remote, base, patch = _patched(tmp_path, "scripts/fuzzer/walk.py")
    note = push("valkey-ci-agent", str(remote), "main", base, patch, tmp_path / "w", "success")
    assert note == "valkey-ci-agent: pushed 1 commit(s) to main"
    assert _git(remote, "log", "-1", "--format=%s|%an|%cn", "main") == "Change it|t|Sarthak Aggarwal"


@pytest.mark.parametrize("path", [".github/workflows/fuzzer-walk.yml", "scripts/ai/claude_code.py",
                                  "scripts/fuzzer/maintain.py"])
def test_a_patch_touching_a_protected_path_is_not_pushed(tmp_path, path):
    remote, base, patch = _patched(tmp_path, path)
    note = push("valkey-ci-agent", str(remote), "main", base, patch, tmp_path / "w", "success")
    assert note == f"valkey-ci-agent: not pushed, it changes protected paths: {path}"
    assert _git(remote, "rev-parse", "main") == base


def test_a_patch_whose_tests_failed_or_whose_branch_moved_is_not_pushed(tmp_path):
    remote, base, patch = _patched(tmp_path, "a.py")
    assert push("x", str(remote), "main", base, patch, tmp_path / "w", "failure") == \
        "x: not pushed, its tests did not pass (failure)"
    assert push("x", str(remote), "main", base, patch, tmp_path / "w", "") == "x: not pushed, its tests did not pass (not run)"
    assert push("x", str(remote), "main", "f" * 40, patch, tmp_path / "w", "success") == \
        "x: not pushed, main moved since Claude started"


def test_gather_writes_the_state_the_open_walk_issues_and_the_failed_jobs_log_tails(tmp_path):
    state = tmp_path / "walk-state"
    (state / "state").mkdir(parents=True)
    (state / "PROGRESS.md").write_text("# Progress\n")
    (state / "state" / "valkey.json").write_text("{}")
    issues = [{"number": 1, "title": "[fuzzer-walk] crash", "url": "u1", "createdAt": "t", "body": "b",
               "comments": [{"author": {"login": "a"}, "createdAt": "t", "body": "Vetted: real"}]},
              {"number": 2, "title": "[fuzzer-walk] hang", "url": "u2", "createdAt": "t", "body": "b", "comments": []},
              {"number": 3, "title": "Walk maintenance log", "url": "u3", "createdAt": "t", "body": "", "comments": []}]
    runs = [{"databaseId": 7, "conclusion": "failure", "status": "completed"}]
    jobs = {"jobs": [{"databaseId": 70, "name": "walk (a)", "conclusion": "failure"},
                     {"databaseId": 71, "name": "walk (b)", "conclusion": "success"}]}

    def gh(*args):
        return {"issue": json.dumps(issues), "api": "\n".join(f"line {i}" for i in range(300)),
                "run": json.dumps(runs if args[1] == "list" else jobs)}[args[0]]

    out = tmp_path / "in"
    maintain.gather(out, walk_repo="o/ci", fuzzer_repo="o/fz", state=state, gh=gh)
    assert (out / "PROGRESS.md").read_text() == "# Progress\n" and (out / "state" / "valkey.json").is_file()
    written = json.loads((out / "issues.json").read_text())
    assert [(i["number"], i["vetted"]) for i in written] == [(1, True), (2, False)]
    run = json.loads((out / "runs.json").read_text())[0]
    assert [job.get("log") for job in run["jobs"]] == ["logs/7-70.log", None]
    tail = (out / "logs" / "7-70.log").read_text().splitlines()
    assert (len(tail), tail[0], tail[-1]) == (200, "line 100", "line 299")


# --- The workflow --------------------------------------------------------------


def _workflow() -> dict:
    return yaml.load(Path(".github/workflows/fuzzer-walk-maintain.yml").read_text(encoding="utf-8"),
                     Loader=yaml.BaseLoader)


def _step(job: dict, name: str) -> dict:
    return next(step for step in job["steps"] if step.get("name") == name)


def test_the_workflow_runs_claude_without_github_writes_and_applies_its_results_after():
    workflow = _workflow()
    assert list(workflow["on"]) == ["workflow_dispatch"] and workflow["permissions"] == {}
    assert workflow["concurrency"] == {"group": "fuzzer-walk-maintain", "cancel-in-progress": "false"}
    jobs = workflow["jobs"]
    assert list(jobs) == ["gate", "gather", "claude", "verify", "apply"]
    assert {name: job["permissions"] for name, job in jobs.items()} == {
        "gate": {"actions": "read"},
        "gather": {"contents": "read", "actions": "read"},
        "claude": {"contents": "read", "id-token": "write"},
        "verify": {"contents": "read"},
        "apply": {"contents": "write"},
    }
    # Every secret each job sees: Claude's and the tests' jobs none but the AWS role, the writes only in apply.
    secrets = {name: set(re.findall(r"secrets\.(\w+)", yaml.safe_dump(job))) for name, job in jobs.items()}
    assert secrets == {"gate": set(), "gather": set(), "claude": {"AWS_ROLE_ARN"}, "verify": set(),
                       "apply": {"REHEARSAL_PAT"}}
    assert "github.token" not in yaml.safe_dump(jobs["claude"]) + yaml.safe_dump(jobs["verify"])
    checkouts = [s for job in jobs.values() for s in job["steps"] if "actions/checkout" in s.get("uses", "")]
    assert checkouts and all(s["with"]["persist-credentials"] == "false" for s in checkouts)

    # The gate: never under valkey-io, and a run maintained in the last 6 hours skips this one.
    assert jobs["gate"]["if"] == "github.repository_owner != 'valkey-io'"
    gate = _step(jobs["gate"], "Skip when the walk was maintained in the last 6 hours")
    assert gate["env"] == {"GH_TOKEN": "${{ github.token }}"}
    assert "gh run list" in gate["run"] and "6 hours ago" in gate["run"] and '.name == "claude"' in gate["run"]
    assert (jobs["gather"]["needs"], jobs["gather"]["if"]) == ("gate", "needs.gate.outputs.go == 'true'")
    assert jobs["claude"]["needs"] == "gather" and jobs["verify"]["needs"] == "claude"

    step = _step(jobs["claude"], "Maintain the walk")
    assert step["env"] == {"CI_AGENT_AWS_ROLE_ARN": "${{ secrets.AWS_ROLE_ARN }}"}
    assert int(step["timeout-minutes"]) < int(jobs["claude"]["timeout-minutes"]) <= 90
    assert "maintain run --work work" in step["run"]

    # The pushes and the issues' writes follow a guard against valkey-io, from this run's own commit.
    apply_job = jobs["apply"]
    assert apply_job["steps"][0]["name"] == "Refuse any valkey-io target"
    assert '"${target}" != valkey-io/*' in apply_job["steps"][0]["run"]
    assert "nocasematch" in apply_job["steps"][0]["run"]
    assert "ref" not in apply_job["steps"][1]["with"]
    env = workflow["env"]
    assert (env["CI_AGENT_REPOSITORY"], env["FUZZER_REPOSITORY"]) == ("sarthakaggarwal97/valkey-ci-agent", FUZZER_REPO)
    ci_agent, fuzzer = _step(apply_job, "Push valkey-ci-agent"), _step(apply_job, "Push valkey-fuzzer")
    assert ci_agent["env"]["TOKEN"] == "${{ github.token }}" and fuzzer["env"]["TOKEN"] == "${{ secrets.REHEARSAL_PAT }}"
    assert ci_agent["env"]["VERIFIED"] == "${{ needs.verify.outputs.valkey-ci-agent }}"
    assert '--repo "${CI_AGENT_REPOSITORY}"' in ci_agent["run"] and '--repo "${FUZZER_REPOSITORY}"' in fuzzer["run"]
    applying = _step(apply_job, "Apply the vetting and post the report")
    assert applying["env"]["TARGET_TOKEN"] == "${{ secrets.REHEARSAL_PAT }}"
    assert '--repo "${FUZZER_REPOSITORY}"' in applying["run"]
    # The tests run again where Claude's code has no secret, and their outcomes gate the pushes.
    assert jobs["verify"]["outputs"] == {"valkey-ci-agent": "${{ steps.ci-agent.outcome }}",
                                         "valkey-fuzzer": "${{ steps.fuzzer.outcome }}"}
