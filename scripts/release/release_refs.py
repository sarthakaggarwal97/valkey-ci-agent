"""Shared ref conventions between the release-notes cut and the controller.

The release-notes cut pushes its PR from ``agent/release-cut/<version>-<stage>``
(:data:`scripts.release_notes.release_cut.PREP_BRANCH_PREFIX`). The controller
identifies the notes PR (and reads the version and stage it pins) from that
head branch. Building the regex from the cut's own constant keeps the two
modules from drifting apart.

Also home to the one tag-resolution helper every release module shares.
"""

from __future__ import annotations

import re
from typing import Any

from github.GithubException import GithubException

from scripts.common.github_client import retry_github_call
from scripts.release_notes.release_cut import PREP_BRANCH_PREFIX

# Matches a notes prep branch; group 1 is the version, group 2 the stage.
NOTES_PREP_BRANCH_RE = re.compile(
    rf"^{re.escape(PREP_BRANCH_PREFIX)}/(\d+\.\d+\.\d+)-(ga|rc[1-9]\d*)$"
)


def resolve_tag_commit(repo: Any, tag: str) -> str:
    """The commit SHA *tag* points at, dereferencing annotated tags.

    "" when the tag does not exist. The single choke point for "does this
    tag exist and where": stray-tag detection, publication post-verify,
    published-phase pinning, and the docs/helm-chart verifiers all ask the
    same question.
    """
    try:
        ref = retry_github_call(
            lambda: repo.get_git_ref(f"tags/{tag}"),
            retries=2, description=f"resolve tag {tag}",
        )
    except GithubException as exc:
        if exc.status == 404:
            return ""
        raise
    if ref.object.type == "commit":
        return ref.object.sha
    tag_obj = retry_github_call(
        lambda: repo.get_git_tag(ref.object.sha),
        retries=2, description=f"dereference annotated tag {tag}",
    )
    return tag_obj.object.sha


def read_text_file(repo: Any, path: str, ref: Any = None) -> str:
    """UTF-8 contents of *path* in *repo* (at *ref* when given).

    The one place the get_contents/decoded_content/decode chain lives;
    publish (version.h, release notes) and the downstream verifiers
    (hashes README, versions.json, Chart.yaml) all read through it.
    """
    kwargs = {"ref": ref} if ref is not None else {}
    contents = retry_github_call(
        lambda: repo.get_contents(path, **kwargs),
        retries=2, description=f"read {path}" + (f" at {str(ref)[:12]}" if ref else ""),
    )
    return contents.decoded_content.decode("utf-8")


def workflow_handle(gh: Any, repo_name: str, workflow_file: str) -> Any:
    """The workflow object for *workflow_file* in *repo_name*, or None on 404.

    The one place the get_repo -> get_workflow chain lives; qualification,
    build observation, and the publish auto-dispatch all start here.
    """
    repo = retry_github_call(
        lambda: gh.get_repo(repo_name),
        retries=2, description=f"get repo {repo_name}",
    )
    try:
        return retry_github_call(
            lambda: repo.get_workflow(workflow_file),
            retries=2, description=f"get workflow {workflow_file}",
        )
    except GithubException as exc:
        if exc.status == 404:
            return None
        raise
