"""Small immutable values used by the release workflows."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class ReleaseIntent(str, Enum):
    RC = "rc"
    GA = "ga"
    PATCH = "patch"


@dataclass(frozen=True)
class DerivedRelease:
    version: str
    stage: str

    @property
    def tag(self) -> str:
        return self.version if self.stage == "ga" else f"{self.version}-{self.stage}"


@dataclass(frozen=True)
class ReleasePolicy:
    repo: str
    authorized_teams: tuple[str, ...]
    branches: tuple[str, ...]
    checks_workflow: str
    required_checks: tuple[str, ...]

    @property
    def authorized_teams_display(self) -> str:
        return " or ".join(self.authorized_teams)


@dataclass(frozen=True)
class PublishPlan:
    branch: str
    tag: str
    version: str
    stage: str
    sha: str
    body: str
    prerelease: bool
    make_latest: str
    tag_protected: bool | None
    candidate_ci: str = "not checked"
