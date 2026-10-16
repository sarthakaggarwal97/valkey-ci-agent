"""Small domain model shared by every CI-fix front door."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional

from scripts.common.valkey_ci import FILE_TIMEOUT_TEST_NAME, Source


class FixPolicy(str, Enum):
    """What kind of change the agent may make."""

    BACKPORT = "backport"
    GENERAL = "general"


class TargetKind(str, Enum):
    """Where a prepared candidate should be published."""

    UPDATE_PR = "update-pr"
    SUGGEST = "suggest"
    DRAFT_PR = "draft-pr"


class CandidateAction(str, Enum):
    """The concrete action proposed by the fix agent."""

    PATCH = "patch"
    PORT = "port"
    REFUSE = "refuse"


class ResultKind(str, Enum):
    """The externally visible result of one fix attempt."""

    PUSHED = "pushed"
    SUGGESTED = "suggested"
    OPENED = "opened"
    REFUSED = "refused"
    FAILED = "failed"


class FailureType(str, Enum):
    """How the agent classified the observed failure."""

    DETERMINISTIC = "deterministic"
    FLAKY = "flaky"
    INFRASTRUCTURE = "infrastructure"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class FailedTest:
    """One failing Tcl test: its name and ``tests/...tcl`` file.

    ``name`` is "" when a test client timed out outside any test: then only
    the file is known, and the whole file is what failed.
    """

    name: str
    file: str

    @classmethod
    def from_report(cls, name: str, file: str) -> "FailedTest":
        """The test a runtest report or detector issue names."""
        return cls("" if name == FILE_TIMEOUT_TEST_NAME else name, file)

    @property
    def whole_file(self) -> bool:
        return not self.name

    @property
    def unit(self) -> str:
        """``unit/type/incr`` for ``tests/unit/type/incr.tcl``."""
        return self.file.removeprefix("tests/").removesuffix(".tcl")


@dataclass(frozen=True)
class Failure:
    """One exact failed job on one tested commit."""

    repo_full_name: str
    run_id: int
    head_sha: str
    job_id: int
    job_name: str
    run_attempt: int = 1
    base_branch: str = ""
    description: str = ""
    hint: str = ""
    pr_number: int = 0
    issue_number: int = 0
    other_failed_jobs: tuple[str, ...] = ()
    # Tests the front door already knows failed (a detector issue records
    # one). The engine adds the ones the job log shows.
    failed_tests: tuple[FailedTest, ...] = ()
    # The scheduled workflow that failed, or None for a PR's run. Scheduled
    # runs pass ``--accurate`` to the tests.
    source: Source | None = None
    # The failed run's workflow file, without ``.yml`` (``ci``, ``daily``).
    workflow: str = ""

    @property
    def scheduled(self) -> bool:
        return self.source is not None

    @property
    def run_url(self) -> str:
        url = f"https://github.com/{self.repo_full_name}/actions/runs/{self.run_id}"
        if self.run_attempt > 1:
            return f"{url}/attempts/{self.run_attempt}"
        return url


@dataclass(frozen=True)
class FixTarget:
    """A bot branch, contributor PR, or new draft PR destination."""

    kind: TargetKind
    repo_full_name: str
    base_repo_full_name: str = ""
    branch: str = ""
    expected_sha: str = ""
    base_branch: str = ""
    pr_number: int = 0
    issue_number: int = 0
    draft: bool = True


@dataclass(frozen=True)
class OtherIssue:
    """Another open detector issue whose test a patch here might also fix."""

    number: int
    test: FailedTest


@dataclass(frozen=True)
class FixTask:
    """A validated failure paired with its publication destination."""

    failure: Failure
    target: FixTarget
    policy: FixPolicy
    # Other open detector issues whose test this patch may also fix. Each is
    # verified on its own before the draft PR claims to fix it.
    other_issues: tuple[OtherIssue, ...] = ()


@dataclass(frozen=True)
class FixCandidate:
    """A model-proposed patch, upstream port, or reason not to act."""

    action: CandidateAction
    summary: str = ""
    failing_check: str = ""
    root_cause: str = ""
    reasoning: str = ""
    failure_type: FailureType = FailureType.UNKNOWN
    confidence: float = 0.0
    patch: str = ""
    changed_paths: tuple[str, ...] = ()
    port_commit: str = ""
    validation: str = ""
    # A focused command, run from the checkout root, that runs the failing
    # test or check. Code runs it before and after the patch.
    test_command: str = ""
    # What code established about the candidate (local test runs and the
    # review), for the maintainer report. Never written by the agent.
    verification: str = ""
    # Other detector issues this patch was shown to fix, and ones whose test
    # it changed without that being shown. Never written by the agent.
    also_fixes: tuple[int, ...] = ()
    also_touches: tuple[int, ...] = ()


@dataclass(frozen=True)
class FixContext:
    """What code established before the agent starts, for it and the reviewer."""

    # Merged changes that may already fix the failure (``prior_fixes``), and
    # whether the failure gave the search anything to look for.
    prior_fixes: str = ""
    prior_fixes_detail: str = ""
    prior_fixes_searched: bool = False
    # Whether and how the failure reproduced on the unfixed code, and whether
    # code verifies a candidate with that reproduction command.
    reproduction: str = ""
    reproduction_output: str = ""
    verified_by_code: bool = False
    # The repository's test conventions, and its own docs in the checkout.
    guide: str = ""
    guide_paths: tuple[str, ...] = ()


@dataclass(frozen=True)
class FixResult:
    """The final published or reported outcome."""

    kind: ResultKind
    summary: str
    task: FixTask
    candidate: Optional[FixCandidate] = None
    commit_sha: str = ""
    pr_number: int = 0
    pr_url: str = ""


class FrontDoor(str, Enum):
    """Where a fix request came from: it decides how the result is published and reported."""

    PR = "pr"  # a maintainer's command on a PR (scripts/ci_fix/main.py)
    FOLLOW_UP = "follow-up"  # a bot-owned PR's red head (scripts/backport/ci_followup.py)


@dataclass(frozen=True)
class FixPlan:
    """One bound fix request, handed from the plan job to the fix and publish jobs."""

    front_door: FrontDoor
    task: FixTask
    job_log: str = ""
    # PR: the command comment, which gets the outcome as a reaction.
    comment_id: int = 0
    # FOLLOW-UP: the markers that retire the job once its result is terminal.
    markers: tuple[str, ...] = ()


@dataclass(frozen=True)
class FixOutcome:
    """What the fix job hands the publish job: a candidate, or why there is none."""

    candidate: Optional[FixCandidate] = None
    error: str = ""
