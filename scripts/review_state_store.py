"""Durable incremental-review state store for the PR reviewer."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from github.GithubException import GithubException

from scripts.github_client import retry_github_call
from scripts.models import ReviewState, review_state_from_dict, review_state_to_dict

if TYPE_CHECKING:
    from github import Github

logger = logging.getLogger(__name__)

_STORE_BRANCH = "bot-data"
_STORE_FILE = "review-state.json"


class ReviewStateStore:
    """Branch-backed persistence for PR reviewer incremental state."""

    def __init__(
        self,
        github_client: "Github | None" = None,
        repo_full_name: str = "",
    ) -> None:
        self._gh = github_client
        self._repo_name = repo_full_name
        self._states: dict[str, ReviewState] = {}
        self._loaded = False

    @staticmethod
    def _key(repo: str, pr_number: int) -> str:
        return f"{repo}#{pr_number}"

    def load(self, repo: str, pr_number: int) -> ReviewState | None:
        """Load a single PR review state from the durable store."""
        self._ensure_loaded()
        return self._states.get(self._key(repo, pr_number))

    def save(self, state: ReviewState) -> None:
        """Persist or update one PR review state."""
        self._ensure_loaded()
        if not state.updated_at:
            state.updated_at = datetime.now(timezone.utc).isoformat()
        self._states[self._key(state.repo, state.pr_number)] = state
        self._persist()

    def clear(self, repo: str, pr_number: int) -> None:
        """Delete one PR review state if it exists."""
        self._ensure_loaded()
        self._states.pop(self._key(repo, pr_number), None)
        self._persist()

    def to_dict(self) -> dict:
        """Serialize the entire store to JSON-compatible data."""
        return {
            key: review_state_to_dict(state)
            for key, state in self._states.items()
        }

    def from_dict(self, data: dict) -> None:
        """Restore the store from JSON-compatible data."""
        self._states = {
            key: review_state_from_dict(raw_state)
            for key, raw_state in data.items()
        }

    def _ensure_loaded(self) -> None:
        if self._loaded:
            return
        self._load_all()
        self._loaded = True

    def _ensure_store_branch(self, repo) -> None:
        try:
            retry_github_call(
                lambda: repo.get_git_ref(f"heads/{_STORE_BRANCH}"),
                retries=5,
                description=f"check branch {_STORE_BRANCH}",
            )
            return
        except GithubException as exc:
            if exc.status != 404:
                raise
        except FileNotFoundError:
            pass

        base_ref = retry_github_call(
            lambda: repo.get_git_ref(f"heads/{repo.default_branch}"),
            retries=5,
            description=f"load default branch ref {repo.default_branch}",
        )
        retry_github_call(
            lambda: repo.create_git_ref(
                ref=f"refs/heads/{_STORE_BRANCH}",
                sha=base_ref.object.sha,
            ),
            retries=5,
            description=f"create branch {_STORE_BRANCH}",
        )

    def _load_all(self) -> None:
        if not self._gh or not self._repo_name:
            logger.info("No GitHub client; starting with empty review state store.")
            return
        gh = self._gh
        try:
            repo = retry_github_call(
                lambda: gh.get_repo(self._repo_name),
                retries=5,
                description=f"load repository {self._repo_name}",
            )
            contents = retry_github_call(
                lambda: repo.get_contents(_STORE_FILE, ref=_STORE_BRANCH),
                retries=5,
                description=f"load {_STORE_FILE}",
            )
            if isinstance(contents, list):
                raise ValueError("Review state path resolved to a directory.")
            data = json.loads(contents.decoded_content.decode())
            self.from_dict(data)
        except Exception as exc:
            logger.info("Could not load review state store (may not exist yet): %s", exc)
            self._states = {}

    def _persist(self) -> None:
        if not self._gh or not self._repo_name:
            logger.warning("Cannot save review state store: no GitHub client or repo.")
            return
        gh = self._gh
        try:
            repo = retry_github_call(
                lambda: gh.get_repo(self._repo_name),
                retries=5,
                description=f"load repository {self._repo_name}",
            )
            self._ensure_store_branch(repo)
            content = json.dumps(self.to_dict(), indent=2)
            try:
                existing = retry_github_call(
                    lambda: repo.get_contents(_STORE_FILE, ref=_STORE_BRANCH),
                    retries=5,
                    description=f"load {_STORE_FILE}",
                )
            except GithubException as exc:
                if exc.status != 404:
                    raise
                existing = None
            except FileNotFoundError:
                existing = None

            if isinstance(existing, list):
                raise ValueError("Review state path resolved to a directory.")
            if existing is None:
                retry_github_call(
                    lambda: repo.create_file(
                        _STORE_FILE,
                        "Initialize PR review state",
                        content,
                        branch=_STORE_BRANCH,
                    ),
                    retries=5,
                    description=f"create {_STORE_FILE}",
                )
            else:
                retry_github_call(
                    lambda: repo.update_file(
                        _STORE_FILE,
                        "Update PR review state",
                        content,
                        existing.sha,
                        branch=_STORE_BRANCH,
                    ),
                    retries=5,
                    description=f"update {_STORE_FILE}",
                )
        except Exception as exc:
            logger.warning("Failed to persist review state store: %s", exc)
