"""Keep this repository's copies of Valkey's CI workflows current.

For ``unstable`` and every release branch from 7.2 on (the branches Valkey's
Weekly tests), the branch's ``daily.yml`` and ``ci.yml`` are fetched, made
callable (:mod:`scripts.valkey_workflows.normalize`) and written to
``.github/workflows/valkey-copy-<branch>-<workflow>.yml``. A router,
``valkey-copy-job.yml``, takes the branch and workflow as inputs and calls
the matching copy, so a caller needs one static ``uses:``. ``manifest.json``
records each copy's source commit, job names and the jobs with a fix mode,
which is how a failed job's name is mapped back to the copied job that fixes
it.

The weekly ``valkey-workflows-sync.yml`` runs this and opens a pull request
with any change. The pull request names every job that had a copy, or a fix
mode, and lost it: a change upstream that the normalizer no longer
understands would otherwise only show as issues falling back to the bot's
own runner.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable
from urllib.request import Request, urlopen

import yaml

from scripts.valkey_workflows.normalize import FIX_INPUTS, INPUTS, PERMISSIONS, normalize

logger = logging.getLogger(__name__)

REPO = "valkey-io/valkey"
WORKFLOWS = ("daily", "ci")
OLDEST_RELEASE = (7, 2)
WORKFLOW_DIR = Path(".github/workflows")
MANIFEST = Path("scripts/valkey_workflows/manifest.json")
ROUTER = "valkey-copy-job.yml"
_RELEASE_RE = re.compile(r"^(\d+)\.(\d+)$")

Fetch = Callable[[str], tuple[str, dict[str, str]]]
"""``fetch(branch)`` -> (head commit, {workflow: file text}) for that branch."""


@dataclass(frozen=True)
class Source:
    branch: str
    sha: str
    workflow: str
    text: str


def branches(names: list[str]) -> list[str]:
    """``unstable`` and the release branches Weekly tests, newest first."""
    releases = []
    for name in names:
        match = _RELEASE_RE.match(name)
        if match and (int(match.group(1)), int(match.group(2))) >= OLDEST_RELEASE:
            releases.append((int(match.group(1)), int(match.group(2))))
    return ["unstable", *(f"{major}.{minor}" for major, minor in sorted(releases, reverse=True))]


def copy_name(branch: str, workflow: str) -> str:
    return f"valkey-copy-{branch.replace('.', '-')}-{workflow}.yml"


def sync(branch_names: list[str], fetch: Fetch, root: Path = Path(".")) -> list[Path]:
    """Rewrite every copy, the router and the manifest under ``root``; the files written."""
    manifest: dict[str, dict[str, object]] = {}
    written: list[Path] = []
    wanted: set[str] = set()
    for branch in branches(branch_names):
        sha, files = fetch(branch)
        for workflow in WORKFLOWS:
            text = files.get(workflow)
            if text is None:
                logger.warning("%s has no %s.yml; no copy is made", branch, workflow)
                continue
            copy = normalize(text, origin=f"{REPO}@{sha} ({branch}):.github/workflows/{workflow}.yml")
            name = copy_name(branch, workflow)
            wanted.add(name)
            written.append(_write(root / WORKFLOW_DIR / name, copy.text))
            manifest[f"{branch}/{workflow}"] = {
                "branch": branch,
                "workflow": workflow,
                "source_sha": sha,
                "file": name,
                "jobs": copy.jobs,
                "targeted": copy.targeted,
                "fixable": copy.fixable,
            }
    for stale in (root / WORKFLOW_DIR).glob("valkey-copy-*.yml"):
        if stale.name not in wanted and stale.name != ROUTER:
            stale.unlink()
            logger.info("Removed %s: its branch is gone", stale.name)
    written.append(_write(root / WORKFLOW_DIR / ROUTER, router(manifest)))
    written.append(_write(root / MANIFEST, json.dumps(manifest, indent=2, sort_keys=True) + "\n"))
    return written


def router(manifest: dict[str, dict[str, object]]) -> str:
    """The one workflow callers use: it calls the copy for ``branch`` and ``workflow``."""
    inputs = {
        "branch": {"description": "unstable or a release branch", "required": True, "type": "string"},
        "workflow": {"description": "daily or ci", "required": True, "type": "string"},
        **INPUTS,
        **FIX_INPUTS,
    }
    jobs = {}
    for key, entry in manifest.items():
        job_id = "copy-" + key.replace("/", "-").replace(".", "-")
        jobs[job_id] = {
            "if": f"inputs.branch == '{entry['branch']}' && inputs.workflow == '{entry['workflow']}'",
            "uses": f"./{WORKFLOW_DIR.as_posix()}/{entry['file']}",
            "with": {name: f"${{{{ inputs.{name} }}}}" for name in (*INPUTS, *FIX_INPUTS)},
            "secrets": {"AWS_ROLE_ARN": "${{ secrets.AWS_ROLE_ARN }}"},
        }
    workflow = {
        "name": "Valkey copy: run one job",
        "on": {"workflow_call": {
            "inputs": inputs,
            "secrets": {"AWS_ROLE_ARN": {"description": "The role fix mode assumes for the model",
                                         "required": False}},
        }},
        "permissions": PERMISSIONS,
        "jobs": jobs,
    }
    header = "# Generated by scripts/valkey_workflows/sync.py. Do not edit.\n"
    return header + yaml.safe_dump(workflow, sort_keys=False, width=10_000)


def lost_jobs(before: dict[str, Any], after: dict[str, Any]) -> list[str]:
    """The copied jobs ``before`` has and ``after`` lost, or whose fix mode it lost."""
    lost = []
    for key, entry in sorted(before.items()):
        now = after.get(key) or {}
        for job in sorted(entry.get("jobs") or {}):
            if job not in (now.get("jobs") or {}):
                lost.append(f"`{key}` `{job}`: no longer copied")
            elif job in (entry.get("fixable") or {}) and job not in (now.get("fixable") or {}):
                lost.append(f"`{key}` `{job}`: lost its fix mode")
    return lost


def _write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists() or path.read_text(encoding="utf-8") != text:
        path.write_text(text, encoding="utf-8")
        logger.info("Wrote %s", path)
    return path


# --- GitHub ------------------------------------------------------------------


def _get(url: str, token: str) -> bytes:
    headers = {"Accept": "application/vnd.github+json", "User-Agent": "valkey-ci-agent"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    with urlopen(Request(url, headers=headers), timeout=60) as response:  # noqa: S310 - fixed https host
        return response.read()


def github_branch_names(token: str) -> list[str]:
    names: list[str] = []
    page = 1
    while True:
        batch = json.loads(_get(f"https://api.github.com/repos/{REPO}/branches?per_page=100&page={page}", token))
        names += [item["name"] for item in batch]
        if len(batch) < 100:
            return names
        page += 1


def github_fetch(token: str) -> Fetch:
    def fetch(branch: str) -> tuple[str, dict[str, str]]:
        sha = json.loads(_get(f"https://api.github.com/repos/{REPO}/commits/{branch}", token))["sha"]
        files = {}
        for workflow in WORKFLOWS:
            url = f"https://raw.githubusercontent.com/{REPO}/{sha}/.github/workflows/{workflow}.yml"
            try:
                files[workflow] = _get(url, "").decode("utf-8")
            except OSError as exc:
                logger.warning("Could not fetch %s for %s: %s", workflow, branch, exc)
        return sha, files

    return fetch


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--token", default="", help="GitHub token (optional; raises the API rate limit)")
    parser.add_argument("--report", type=Path, help="Write the jobs the sync lost here, as Markdown")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    before = json.loads(MANIFEST.read_text(encoding="utf-8")) if MANIFEST.exists() else {}
    sync(github_branch_names(args.token), github_fetch(args.token))
    lost = lost_jobs(before, json.loads(MANIFEST.read_text(encoding="utf-8")))
    for line in lost:
        logger.warning("Lost %s", line.replace("`", ""))
    if args.report:
        args.report.write_text(
            "".join(f"- {line}\n" for line in lost) if lost else "", encoding="utf-8",
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
