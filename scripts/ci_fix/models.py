"""Small domain model shared by every CI-fix front door."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional

from scripts.common.valkey_ci import FILE_TIMEOUT_TEST_NAME, Source, is_unit_test_file


class FixPolicy(str, Enum):
    """What kind of change the agent may make."""

    BACKPORT = "backport"
    GENERAL = "general"


class TargetKind(str, Enum):
    """Where a prepared candidate should be published."""

    UPDATE_PR = "update-pr"
    SUGGEST = "suggest"
    DRAFT_PR = "draft-pr"


class IssuePublication(str, Enum):
    """How a fix for a detector issue is published.

    ``comment`` posts the patch on the issue for a maintainer to apply;
    ``draft-pr`` pushes a branch and opens a draft PR. The
    ``CI_FIX_ISSUES_PUBLISH`` repository variable chooses; comment is the
    default while the bot earns its PRs.
    """

    COMMENT = "comment"
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

    @property
    def successful(self) -> bool:
        """Whether the attempt published a fix worth reporting to the target."""
        return self in (
            type(self).PUSHED,
            type(self).SUGGESTED,
            type(self).OPENED,
        )


# A failed attempt (an agent crash, the time running out) says nothing about
# the failure. A detector issue and a bot-owned PR's follow-up try it again,
# and give up after this many failed attempts.
MAX_FAILED_ATTEMPTS = 2


class FailureType(str, Enum):
    """How the agent classified the observed failure."""

    DETERMINISTIC = "deterministic"
    FLAKY = "flaky"
    INFRASTRUCTURE = "infrastructure"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class FailedTest:
    """One failing test: its name and file.

    A Tcl test's ``tests/...tcl`` file, or a C unit test's (``Suite.Test`` in
    ``src/unit/test_x.cpp``; by base name alone until the checkout locates
    it). ``name`` is "" when a test client timed out outside any test: then
    only the file is known, and the whole file is what failed.
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
    def is_unit_test(self) -> bool:
        """A C unit test (Google Test), run by name, not through runtest."""
        return is_unit_test_file(self.file)

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
    source: Optional[Source] = None
    # The failed run's workflow file, without ``.yml`` (``ci``, ``daily``).
    workflow: str = ""
    # The commit whose workflow file defined the failed job, when it is not
    # the commit the fix is prepared on: a run of every release branch runs
    # the default branch's jobs on each of them.
    workflow_sha: str = ""

    @property
    def scheduled(self) -> bool:
        return self.source is not None

    @property
    def in_ci(self) -> bool:
        """Whether a CI run observed the failure.

        Otherwise the bot observed it itself, running a test it wrote from an
        issue (scripts/factory): there is no run, job or job log, only the
        test's own output.
        """
        return self.run_id > 0

    @property
    def run_url(self) -> str:
        if not self.in_ci:
            return ""
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
    # Set for a backport sweep PR: its fix is prepared and run on ``branch``
    # at ``expected_sha``, but pushed to this branch, whose pull request into
    # ``branch`` collects the sweep's CI fixes for review apart from its
    # backports.
    fix_branch: str = ""


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
    # Paths a fix may not change: the files of the test it is judged by, when
    # the bot wrote that test itself (scripts/factory). A patch that touches
    # one goes back to the agent.
    frozen_paths: tuple[str, ...] = ()


@dataclass(frozen=True)
class FixCandidate:
    """A model-proposed patch, upstream port, or reason not to act."""

    action: CandidateAction
    summary: str = ""
    failing_check: str = ""
    root_cause: str = ""
    reasoning: str = ""
    failure_type: FailureType = FailureType.UNKNOWN
    patch: str = ""
    changed_paths: tuple[str, ...] = ()
    port_commit: str = ""
    validation: str = ""
    # The agent's commit subject for a patch: what the change does.
    title: str = ""
    # The merged upstream commit a patch adapts to this branch, checked by
    # code; set only where an upstream change comes first (FixContext.upstream_first).
    adapts: str = ""
    # The tests the agent read in the log as the ones that failed. Code runs
    # them when it could name none itself: a valgrind or sanitizer report,
    # or a test client's exception, names no test on its result line.
    failing_tests: tuple[FailedTest, ...] = ()
    # What code established about the candidate (local test runs and the
    # review), for the maintainer report. Never written by the agent.
    verification: str = ""
    # Other detector issues whose test the patch changed, without showing
    # that it fixes them. Never written by the agent.
    also_touches: tuple[int, ...] = ()
    # Never run: nothing the bot has can run the failing check, so the change
    # was reviewed, and at most built (FixContext.reviewed_only, only_built).
    # A patch like that is posted for a person to apply, never pushed. Never
    # written by the agent.
    not_run: bool = False
    # The test the bot wrote to demonstrate the failure, as its own patch,
    # when the bot wrote one (scripts/factory): ``patch`` then holds the test
    # and the fix together. Kept apart so a report can show the test alone,
    # and a replay can run it against the fix a person wrote. Never written
    # by the agent.
    evidence_patch: str = ""


@dataclass(frozen=True)
class FixContext:
    """What code established before the agent starts, for it and the reviewer."""

    # Merged changes that may already fix the failure (``prior_fixes``), and
    # whether the failure gave the search anything to look for.
    prior_fixes: str = ""
    prior_fixes_detail: str = ""
    prior_fixes_searched: bool = False
    # The failing test file's recent commits, with the diff of the one that
    # added the test (the edit agent has no git).
    test_history: str = ""
    # The default branch's commits to the failure's files that the checkout
    # lacks, when one of those may fix it (``upstream_first``).
    unported_changes: str = ""
    # Every commit the default branch has and the working commit lacks, with
    # the files it changes: the fix may be in a file the failure does not name.
    unported_index: str = ""
    # What the bot's pull requests into the other release branches changed in
    # the failure's files: one failure is fixed the same way on every branch.
    sibling_changes: str = ""
    # The fixes already made for this sweep PR, waiting in its CI-sweep PR and
    # not on the working commit: a failure is not fixed twice.
    pending_fixes: str = ""
    # How code verifies a candidate, and whether it does.
    verification: str = ""
    verified_by_code: bool = False
    # A change already merged upstream comes first: a port, or a patch that
    # adapts one; a fix of the branch's own only where none fixes the failure.
    # Set on a backport branch, and for a failure no command here can run.
    upstream_first: bool = False
    # Nothing here can run the failing check and no upstream change can be
    # ported, so a fix is reviewed and published as not run.
    reviewed_only: bool = False
    # Nothing here can run the failing check, but an upstream change may fix
    # it: a candidate is built and reviewed, not run.
    only_built: bool = False
    # The bot's guide for the repository, ending with the repository's own
    # guidelines as its default branch has them.
    guide: str = ""


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


@dataclass(frozen=True)
class IssueFailure:
    """The failure occurrence recorded in a detector issue body."""

    number: int
    run_id: int
    test_name: str = ""
    test_file: str = ""
    job_name: str = ""
    fingerprint: str = ""

    @property
    def failed_tests(self) -> tuple[FailedTest, ...]:
        if not (self.test_name and self.test_file):
            return ()
        return (FailedTest.from_report(self.test_name, self.test_file),)

    def describe(self, job: str, source: Source) -> str:
        if self.job_name and self.test_name:
            return f"C unit test `{self.test_name}` (`{self.test_file}`) failed in {source.label} job `{job}`."
        if self.job_name:
            return f"The {source.label} job `{job}` failed without a recorded test failure."
        if self.test_name == FILE_TIMEOUT_TEST_NAME:
            return (
                f"A test client running `{self.test_file}` timed out without "
                f"reporting a test name in {source.label} job `{job}`."
            )
        return (
            f"`{self.test_name}` in `{self.test_file}` failed in {source.label} job "
            f"`{job}`."
        )


class IssueKind(str, Enum):
    """What an issue asks for, which decides what the test the bot writes shows."""

    BUG = "bug"  # a test that fails because of the bug, and passes once it is fixed
    FEATURE = "feature"  # tests of the agreed behaviour, which fail until it exists


@dataclass(frozen=True)
class IssueComment:
    """One comment on an issue, by a committer."""

    author: str
    body: str


@dataclass(frozen=True)
class IssueBrief:
    """An issue as it was when a committer labelled it for a fix (scripts/factory).

    Pinned by the plan job, so the fix job reads what the committer read and
    nothing added since. Only committers' comments are kept: anyone can
    comment, and the brief is what the model works from.
    """

    number: int
    title: str
    body: str
    comments: tuple[IssueComment, ...] = ()
    kind: IssueKind = IssueKind.BUG
    # The issue is a crash report, so a test that crashes the server shows the
    # bug. Otherwise a crash is a finding nobody expected, and stops the run.
    crash_report: bool = False
    # The timeline event that applied the label: a request's identity. A label
    # removed and applied again is a new request.
    label_event_id: int = 0
    labelled_by: str = ""
    labelled_at: str = ""


class FrontDoor(str, Enum):
    """Where a fix request came from: it decides how the result is published and reported."""

    ISSUE = "issue"  # a Test Failure Detector issue (scripts/ci_fix/issues.py)
    PR = "pr"  # a maintainer's command on a PR (scripts/ci_fix/main.py)
    FOLLOW_UP = "follow-up"  # a bot-owned PR's red head (scripts/backport/ci_followup.py)
    FACTORY = "factory"  # an issue a committer labelled for a fix (scripts/factory/main.py)


@dataclass(frozen=True)
class FixPlan:
    """One bound fix request, handed from the plan job to the fix and publish jobs."""

    front_door: FrontDoor
    task: FixTask
    job_log: str = ""
    # ISSUE: the occurrence the issue records; publication rechecks it.
    issue: Optional[IssueFailure] = None
    # PR: the command comment, which gets a success reaction after publication.
    comment_id: int = 0
    # FOLLOW-UP: successful-result markers and identities for silent attempts.
    markers: tuple[str, ...] = ()
    # FACTORY: the issue as it was when labelled, which the fix job works from.
    brief: Optional[IssueBrief] = None
    # Set by the publish job when the fix job failed before the engine started
    # (a runner that could not install or build): such an attempt says nothing
    # about the failure, so no front door counts it.
    never_started: bool = False


@dataclass(frozen=True)
class FixOutcome:
    """What the fix job hands the publish job: a candidate, or why there is none."""

    candidate: Optional[FixCandidate] = None
    error: str = ""
    # The engine ran. An outcome written for it by the job's own steps, or
    # none at all, is a job that failed before it could.
    started: bool = False
