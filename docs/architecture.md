# Architecture

The Valkey CI Agent runs workflows that act on Valkey repositories defined in
the central `repos.yml` registry. Five workflows are active today: backports,
fuzzer monitoring, and the test-failure detector (scheduled), and the CI
test-fix bot and release-notes cut (on-demand).

## Layers

```text
scripts/
  backport/    Backport workflow
  fuzzer/      Fuzzer monitor workflow
  test_failure_detector/ Test Failure Detector workflow
  ci_fix/      CI test-fix bot
  release_notes/ Release-notes cutter: AI notes + version bump
  cve_scan/    CVE scan + verified rebuild workflow (build-and-prove)
  ai/          Claude Code subprocess orchestration
  common/      Shared infrastructure
repos.yml      Registry of repos, release branches, and project boards
```

## Backport Flow

```text
sweep.py (daily cron or manual dispatch)
  -> reads repos.yml and fans out one job per {repo, branch}
  -> discovers PRs from each branch's GitHub Project board
  -> for each registered release branch:
      source_plan.py -> resolves merge/squash sources; rejects rebase merges
      candidate_apply.py -> applies one candidate and resolves conflicts with Claude Code
      pr_creator.py -> opens/updates PR on the upstream repo
```

Validation first runs the registry's optional `validation_setup_commands`,
then validates each cherry-pick against its own pre-candidate commit. Generic
`validation_rules` add commands by path; a repository `validation_profile` can
derive commands from exact changed files. Valkey core uses this for candidate
range whitespace checks, release-workflow-aware changed-file clang-format,
direct Tcl tests in their required harness mode, subsystem suites, C/C++ unit
tests, and targeted reply-schema coverage.
`generated_file_rules` run deterministic generators twice, allow only declared
tracked outputs, and amend converged output into the candidate commit. The
sweep branch is kept green: a cherry-pick is only kept if the whole branch
still validates, and a failure is reset off the branch so it can never block
later candidates. The
run keeps up to two validated cherry-picks (`--max-candidates 2`) and records
skipped or failed candidates in the PR's "Needs attention" section without
committing them. When `repair_validation_failures` is enabled, Claude Code
gets one edit-only repair attempt scoped to the backport diff before a failing
cherry-pick is dropped. The repair may not delete changed tests. If a newly
added test is structurally unsupported by the target branch's harness, a
separate sandboxed adaptation may edit existing branch-native tests only. The
unsupported path and added files belonging only to that harness are supplied as
adaptation context, then removed and committed only after the complete
validation plan passes. Independently mapped tests are retained. No safe
adaptation means the candidate fails closed. Repos with no
`build_commands` configured rely on
upstream CI for verification.

### Poll

The daily sweep tops a rolling backport PR up to `--max-candidates` validated
cherry-picks and then waits for the next cron tick, so a merged sweep PR is not
topped back up until the following day. The poll workflow (`backport-poll.yml`)
closes that gap by starting hourly and polling immediately, then once more 30
minutes later inside the same runner. For each registered `{repo, branch}` it
runs the same sweep, but only when no sweep PR is currently open for that
branch:

```text
poller.py (short cron or manual dispatch)
  -> reads repos.yml and fans out one job per {repo, branch}
  -> find_existing_pr(...) -> open sweep PR for this branch?
       yes -> skip; a human is reviewing it
       no  -> run_backport_sweep(...) opens a fresh PR
```

The open-PR check is the entire state model: a merge closes the sweep PR, the
next poll finds the gap and tops the board back up, and the new PR locks the
branch again until it too merges. Poll, daily sweep, and automatic CI follow-up
share the `backport-branch-mutation-{repo}-{branch}` concurrency group so they
never race for the same rolling branch. The group uses the maximum pending
queue so a later scheduled writer cannot replace an already-waiting sweep or
follow-up. Manual dispatches are one-shot; only scheduled runs use the
sustained in-run cadence.

### Automatic CI Follow-up

`backport-ci-followup.yml` covers the state the sweep poller intentionally
leaves alone: an open bot-owned PR whose normal repository CI has completed.
That is the sweep PR, and equally a CI fix this engine pushed or a draft PR it
opened -- nothing else watches whether their own CI goes red. The
registry must explicitly enable `automatic_ci_followup`. During the initial
rollout it is dispatch-only; scheduling can be enabled after manual trials.
Each branch leg:

```text
ci_followup.py
  -> find the open bot-owned PRs into <target> whose head branch this engine
     writes: agent/backport/sweep/<target> first, then backport/,
     agent/backport/ and agent/ci-fix/ heads
  -> require App ownership and exact repo/base/branch/current SHA
  -> wait until all workflow runs for that current head are complete
  -> consider Actions jobs only; checks owned by other Apps (DCO, Codecov)
     are never visible here
  -> discard the informational Actions jobs named in ci_followup_ignored_jobs
  -> skip logical jobs already recorded in hidden result-comment markers
  -> stop after three attempts on the current head
  -> plan one FixTask for the exact prioritized failed job
  -> run it like every other fix (ci_fix/run.py, ci-fix-run.yml)
       one diagnose-and-edit session -> patch | upstream port | refusal
       (backport policy for a backport head, general policy otherwise)
       descendant-only commit plus exact-head lease; no rewrite or DCO sign-off
  -> post the outcome and claim markers; wait for a new head before another fix
```

The automated caller does not impersonate a maintainer comment and does not
weaken the CI-fix engine. It replaces the human-team membership gate with a
machine gate that requires the exact bot-owned sweep PR, branch, and SHA. A
moved head cannot be pushed because the shared push path independently
requires the remote head to equal the failed SHA. The pushed commit starts the
target PR's normal CI; that CI and human review remain authoritative.

### Eligibility

`backport-eligibility.yml` runs at 00:30 UTC and looks at every PR merged to
the development branch on the last three UTC days, so a day one run missed is
covered by the next. It suggests backports; putting a PR on a project board
stays a maintainer decision.

```text
eligibility.py
  -> GraphQL search: PRs merged into the default branch in that window
  -> skip PRs that already carry the eligibility marker comment
  -> git blame the lines each PR modifies or deletes, and the lines on
     either side of what it only inserts, at its parent, looking through
     .git-blame-ignore-revs
       PR that only adds files -> not eligible
  -> per registered branch, a commit that introduced those lines is
     an ancestor, or its PR was backported there -> candidate branch
       drop branches the PR already landed on or is queued on
  -> Claude (no tools) sees the diff and which branches carry each
     introducing commit; answers backport yes/no and a subset of the
     candidate branches
  -> one marker comment per eligible PR naming the branches and projects
```

History bounds the branches and the model can only narrow them, so a fix to a
feature that arrived after a release line forked is not offered to that line
even when it touches an older signature. Whatever history cannot answer (a
PR too large to blame, an unreadable branch, a git failure) and a missing or
unparseable verdict post nothing for that PR and fail the run; the other PRs
are still judged, and comments go out after each model batch.

### Entry Points

- `scripts/backport/sweep.py` - daily sweep across registered repos and release branches
- `scripts/backport/eligibility.py` - daily backport-eligibility comments on newly merged PRs
- `scripts/backport/poller.py` - short-cron poll that sweeps a branch only when no sweep PR is open
- `scripts/backport/ci_followup.py` - guarded current-head CI follow-up for open bot-owned PRs (plan and report)
- `scripts/backport/main.py` - single-PR backport (manual dispatch)
- `scripts/backport/matrix.py` - GitHub Actions matrix generation from `repos.yml`
- `scripts/backport/registry.py` - typed registry loader and validation
- `scripts/backport/sweep_*.py` - focused sweep support modules:
  typed sweep results, Git workspace operations, GitHub PR operations,
  GraphQL access, validation command execution, and Markdown reporting

## Fuzzer Flow

```text
fuzzer/main.py (cron every 4 hours)
  -> common.workflow_artifacts.ArtifactClient.list_recent_runs(...)
  -> FuzzerRunAnalyzer.analyze(run)
       common.workflow_artifacts -> download the artifact bundle
       analyzer._scan_logs() -> deterministic regex pass
       analyzer._invoke_claude() -> drop artifacts in a tempdir,
                                    common.git_clone -> shallow-clone
                                    valkey + valkey-fuzzer at the tested
                                    SHAs, run Claude under
                                    fuzzer_analysis_readonly profile,
                                    parse JSON verdict
       common.incidents.compute_fingerprint() -> stable hash over the
                                    normalized anomaly shapes
  -> common.issue_dedup.IssueDedupPublisher.upsert(...)
       fuzzer.issue_renderer.render_for(analysis) -> title/body/comment
```

Claude is given `Read,Grep,Glob` only - no edits, no shell, no network. The
clones give Claude line-level access so it can grep for assertion text or
crash handlers in `valkey/src/` to distinguish known-benign asserts from new
crashes. If a clone fails, the prompt tells Claude not to cite source line
numbers and the analyzer falls back to artifact-only analysis. If Claude
itself fails, the analyzer falls back to deterministic findings and labels
the verdict `needs-human-triage` rather than silently reporting "normal".
If the fuzzer run produced no artifact bundle the analyzer surfaces that
as an error rather than triaging from raw logs.

Unlike the backport flow, the fuzzer monitor never writes to `valkey-io/valkey`
or `valkey-io/valkey-fuzzer` source - its only side effect is creating or
updating issues on `valkey-fuzzer`.

### Entry Points

- `scripts/fuzzer/main.py` - CLI entry point (cron / manual dispatch)
- `scripts/fuzzer/analyzer.py` - orchestration, deterministic scan, Claude Code integration
- `scripts/fuzzer/issue_renderer.py` - fuzzer-specific title/body/comment rendering
- `scripts/fuzzer/models.py` - typed dataclasses for the analysis pipeline

## CI Fix Flow

The CI-fix engine deliberately uses the same publication model as backports:
prepare one candidate, put it on a bot-owned PR branch, and rely on the target
repository's normal PR CI plus human review. It does not create a separate
verification verdict.

Three front doors construct the same typed `FixTask`, and every fix then runs
the same three jobs (`ci_fix/run.py`, `ci-fix-run.yml`): the front door's plan
binds one exact failed job (a `FixPlan`); the fix runs inside that failed job
itself, from this repository's copy of Valkey's workflow (`ci_fix/copies.py`
finds it), after the job's own setup and build and with its own build and test
commands, or on the bot's runner when no copy can host it; and the front
door's publication and report follow. The fix job holds no GitHub token. The
plan and the fix's outcome pass between the jobs as JSON (`ci_fix/state.py`).
A front door is a planner and a reporter:

- `ci_fix/main.py` (`ci-fix.yml`) handles
  `@valkeyrie-ops fix [<run-or-job-url>] [hint]` on a PR. The manual
  `ci_fix/comment_poll.py` trigger scans recent comments once, atomically claims
  authorized commands with a reaction, and dispatches `ci-fix.yml`; it contains
  no diagnosis or publication logic.
- `backport/ci_followup.py` (`backport-ci-followup.yml`) selects one unhandled
  failure on the exact current head of an open bot-owned PR into a registered
  branch (sweep PR, pushed CI fix, or draft fix PR).
- `ci_fix/issues.py` (`ci-fix-issues.yml`) handles one explicitly selected Test
  Failure Detector issue (Daily, or Weekly on a release branch) and opens a
  draft PR against the branch that run tested when a candidate is found; it
  rechecks the issue right before publication.
  `ci_fix/issue_dispatch.py` (`ci-fix-issue-dispatch.yml`) chooses the
  issues: one `ci-fix-issues.yml` run per eligible open Daily issue, with at
  most a configured number of those runs active at once.

The front doors share the state-reading primitives rather than reimplementing
GitHub semantics. `github_runs.jobs_for_run_attempt()` returns one immutable
raw-job snapshot for an exact rerun attempt; job selection, detector URL
mapping, and detector log analysis all derive from that same tuple.
`github_runs.newest_runs_for_head()` owns branch/SHA filtering and superseded
workflow-run collapse. `issue_state.py` owns detector occurrence parsing and
linked-PR inspection.

```text
front-door gate
  -> FixTask
       Failure: repo, run id + attempt, exact failed job id + name, tested SHA
       FixTarget: update bot PR | suggest on contributor PR | open draft PR
       FixPolicy: backport | general

engine.prepare_fix
  -> workflow_artifacts.download_job_log(exact job id)
  -> shallow_clone_at_sha(expected publication commit; full blobless history)
  -> failing tests = front door's (detector issue) + the log's [err] lines
  -> prior_fixes.find_prior_fixes(checkout, tests, tested SHA, issue)
       git log for commits naming the test, changing its file, or
       referencing the issue: landed since the failing run, or on the
       default branch and not yet ported here
  -> reproduce.plan_for(job name, tests) + reproduce(unfixed checkout)
       build like the job, ./runtest --single --only --loops N --fastfail:
       once, looped, looped under load; reproduced | not | unrunnable
  -> loop, at most MAX_ROUNDS:
       agent.run_fix_agent(task, checkout, job log, feedback, FixContext)
         FixContext = existing fixes + reproduction + repo test guide
         one result: port | patch | refuse
       code checks unchanged HEAD, exact declared paths, complete diagnosis,
       patch size, and full port SHA
       a port is cherry-picked (--no-commit) into the checkout
       verify: the reproduction looped with the candidate, or the agent's
       own test command failing without it and passing with it
       agent.review_candidate(candidate, FixContext)
       failure -> feedback to the next round; a port is undone either way

publish.publish_candidate
  -> suggestion:
       revalidate the open PR and post the concrete patch
  -> existing bot PR:
       revalidate PR ownership, branch, and exact head
       apply the patch or cherry-pick the merged upstream commit in a clean clone
       push one descendant with an exact force-with-lease
  -> detector issue:
       revalidate issue occurrence and source-branch head
       create agent/ci-fix/... and open a draft PR against the tested branch
       containing Fixes #N (Weekly: Refs #N, titled [<release branch>])
  -> render the terminal result; never merge
```

`Failure` preserves the exact evidence identity even when publication uses a
newer commit. PR requests require the run's SHA and branch to equal the current
PR head. Detector issues require a scheduled `daily.yml` or `weekly.yml`
occurrence and an exact matching failed job; their candidate is prepared on the
current tip of the branch that run tested (Daily's own branch, or the release
branch a Weekly issue records), with both the tested and working SHAs visible
to the agent. A detector issue's fix reproduces and verifies only that issue's
test; other tests the same job failed have issues of their own.

The agent's structured result contains the failing check, root cause,
rationale, failure type, changed paths, and optional validation notes. Those
notes are reviewer evidence, not a trusted CI conclusion. Code rejects
agent-created commits, a patch whose actual files differ from its declaration,
an incomplete diagnosis, a malformed or unmerged port, a port already present
on the target, and an oversized suggestion.

The publication boundary is intentionally small. The agent subprocess does not
receive GitHub credentials and cannot push. Code reconstructs the candidate in
a clean clone, writes only `backport/...`, `agent/backport/...`, or
`agent/ci-fix/...`, and rechecks the destination immediately before an exact
lease-protected push. Contributor and fork PRs are never changed; they receive
only a patch suggestion. Each fix workflow uses one repository-scoped App token
and one OIDC session for the invocation, matching the existing backport
operating model.

Target behavior is data-driven but deliberately not a plugin framework.
`policy.py` has one exhaustive `TargetKind` capability map covering agent
profile, patch limit, port support, push permission, and branch creation.
`publish.py` has one exhaustive map from `TargetKind` to the three small target
handlers. Adding a target therefore requires an explicit capability record and
publisher, while front doors continue to produce the same `FixTask`.

Within `engine.prepare_fix`, nothing is edited before two facts are
established. `prior_fixes.py` searches the checkout's history (the clone holds
every branch) for merged commits that name the failing test, change its file
or a source file the log names (a compile error's), or reference its issue; a
match on only the leading words of a long test name counts for less. On a
release branch those are port candidates unless
a cherry-pick (`-x` trailer or identical subject) is already there, and on the
default branch one that landed after the failing run means the failure may
already be fixed. `reproduce.py` turns the job name into Valkey's build and
`runtest` flags (from Valkey's `daily.yml`) and runs only the failing tests on
the unfixed code, looped and then with every core busy. A run counts as a
reproduction only when its output shows one of those tests failing, or a
valgrind or sanitizer report; a build break or a harness exception outside the
test is "unrunnable", and so is a pass that ran none of those tests.
Reproduction may use `REPRO_BUDGET_S` (20 minutes) of
the 45-minute `PREPARE_BUDGET_S`.

The agent gets both results and the repository's test conventions
(`ci_fix/guides/<owner>__<repo>.md`, plus the repository's own docs that exist
in the checkout) and prefers porting a fitting existing fix over writing one.
A candidate for a bot PR or a draft PR then goes through a feedback loop of at
most `MAX_ROUNDS` (3) rounds: a port must be merged on the default branch and
is cherry-picked into the checkout (a conflict goes back to the agent, which
may adapt it as a patch); the reproduction is looped with the candidate
applied, under load when that is how it reproduced (10 loops after a failure
on both plain runs, 40 after an intermittent one, 4 under valgrind), and,
when code's reproduction never failed or there was none, the agent's
`test_command`, if given, must fail without the patch
within `TEST_RUNS` (2) or `FLAKY_TEST_RUNS` (10) tries and pass that many
times with it; and a confined read-only `ci_fix_review` session, given the
same conventions and reproduction, must approve it. A failed check or a
rejection is fed back to the agent, with whether the tree still holds its patch
(a port round resets it). A
failure that never reproduced can still be fixed, but the report says the
fix is unproven. Suggestions are reviewed, never run. Every CI-fix agent
(`ci_fix_edit`, which has Bash, `ci_fix_review` and `ci_fix_suggest`) and every
build and test command run as `CI_FIX_AGENT_USER` via
`scripts/common/isolation.py`; that account cannot read the job's
`TARGET_TOKEN`, cannot write inside `.git`, and leaves nothing behind. It owns
the checkout's directory while it runs, so `handed_to` refuses to hand back a
checkout whose `.git` was replaced (a planted config's filter or merge driver
would otherwise run as the workflow user), and verification first removes
every ignored file so a planted binary cannot stand in for the build. `push.py`
refuses any change under `.github/`, so the tokens have no `workflows` scope.

There is no local/Docker workflow interpreter, macOS verification dispatch,
prepare/publish state file, or Daily
reconciliation loop. A pushed commit or newly opened draft PR simply starts
ordinary target-repository CI, which remains the verification authority.

The rollout is manual-first. `ci-fix.yml`, `ci-fix-comment-poll.yml`,
`backport-ci-followup.yml`, and `ci-fix-issues.yml` are dispatch-only;
`ci-fix-issue-dispatch.yml`'s schedule acts only once the
`CI_FIX_ISSUES_AUTO_DISPATCH` variable is `true`.
`ci-fix.yml` and its comment poller are currently scoped operationally to
`valkey-io/valkey`; the shared Python engine is repository-agnostic, while
onboarding another repository still requires the corresponding workflow token
and trigger configuration. Detector issue handling has no registry
enablement setting.

### Entry Points

- `scripts/ci_fix/run.py` - one fix as three jobs (plan, fix, publish) for every front door
- `scripts/ci_fix/main.py` - authorize and plan one PR request, and report its result
- `scripts/ci_fix/comment_poll.py` - finds fix commands on PRs and dispatches `ci-fix.yml`
- `scripts/ci_fix/issues.py` - validate and plan one Daily or Weekly detector issue, recheck it before publication, and report on it
- `scripts/ci_fix/issue_dispatch.py` - start one issue fix per eligible open Daily issue, up to a cap
- `scripts/ci_fix/copies.py` - map a failed job to the copied job that hosts its fix, and that job's inputs
- `scripts/ci_fix/job.sh` - run the fix inside the job that hosts it: the agent's account and checkout
- `scripts/ci_fix/state.py` - JSON state carried between a fix's jobs
- `scripts/ci_fix/engine.py` - exact-log checkout, existing-fix search, reproduction, fix/verify/review loop, publication
- `scripts/ci_fix/prior_fixes.py` - find merged commits that may already fix the failing tests
- `scripts/ci_fix/reproduce.py` - build and loop the failing Tcl tests with their CI job's commands, or the closest
- `scripts/ci_fix/guides/` - per-repository test conventions for the fix agent and its reviewer
- `scripts/valkey_workflows/` - callable per-branch copies of Valkey's `daily.yml`/`ci.yml` with a fix mode (`normalize.py`), their weekly sync, router and job manifest (`sync.py`)
- `scripts/ci_fix/agent.py` - combined diagnosis-and-edit prompt and structured result parser
- `scripts/ci_fix/publish.py` - target revalidation and publication adapter
- `scripts/ci_fix/policy.py` - shared target capabilities, bot identities, and writable namespaces
- `scripts/ci_fix/issue_state.py` - detector occurrence and linked-PR state readers
- `scripts/ci_fix/gate.py` - command parsing, fail-closed team auth, SHA-bound run gating
- `scripts/ci_fix/github_runs.py` - exact-attempt job snapshots and current-head run selection
- `scripts/ci_fix/push.py` - clean-clone patch/port commit and namespace-restricted push
- `scripts/ci_fix/comment.py` - render a pushed fix, suggestion, refusal, or failure
- `scripts/ci_fix/models.py` - `Failure`, `FixTarget`, `FixTask`, `FixCandidate`, and `FixResult`

## AI Layer

```text
runtime.run_agent(profile, prompt, cwd=...)
  -> claude_code.run_claude_code(...)
    -> subprocess: claude --print (Claude Code CLI via Bedrock)
```

Profiles registered today:

- `conflict_resolve_edit_only` - backport conflict resolution (Read/Edit/Bash, writes allowed)
- `fuzzer_analysis_readonly` - fuzzer triage (Read/Grep/Glob only, no writes)
- `ci_fix_edit` - combined CI-fix diagnosis and editing for bot-owned branches
  (Read/Edit/MultiEdit/Write/Grep/Glob/Bash; no commit or push)
- `ci_fix_suggest` - confined combined diagnosis and editing for contributor
  patch suggestions (Read/Edit/MultiEdit/Write/Grep; no Bash or Glob)
- `release_notes_review_edit_only` - one-file current-section release-note edits
  (Read/Edit/MultiEdit/Grep/Glob; no shell or file creation)

## Common Infrastructure

Workflow-agnostic helpers in `scripts/common/`:

- `git_auth.py` - GIT_ASKPASS credential helper
- `github_client.py` - retry wrapper for GitHub API
- `text_utils.py` - ANSI stripping for log scanning
- `workflow_artifacts.py` - list and download GitHub Actions workflow runs and
  their uploaded artifact bundles, plus exact job-console logs. Used by the
  fuzzer flow (artifacts) and the CI-fix flow (one bound failed-job log).
- `git_clone.py` - `shallow_clone_at_sha(repo, dest, sha)` - defensive
  shallow clone of a public repo at a specific commit, with input
  validation against argument injection. Gives the AI source access at the
  tested or publication-bound SHA in the fuzzer and CI-fix flows.
- `polling.py` - shared one-shot-or-sustained poll loop helpers for scheduled
  pollers that need a predictable in-run cadence.
- `proc.py` - `git_output(...)` (run a git command and return stdout) and
  `filter_env(allowlist)` (the single place that turns an env allowlist into
  a concrete, scrubbed subprocess environment).
- `ai_output.py` - `extract_json_object(stdout, required_key=...)` parses a
  structured verdict out of Claude Code's stream-json output. Shared by the
  fuzzer and CI-fix flows.
- `incidents.py` - `compute_fingerprint(namespace, shapes)` produces a stable
  hash over normalized anomaly shapes for issue deduplication.
- `issue_dedup.py` - `IssueDedupPublisher` creates or updates a GitHub
  issue keyed by a fingerprint marker. Workflows supply the rendered title,
  body, and comment via a small `render(marker, occurrences) -> IssueContent`
  callback; the publisher owns the dedup machinery. When a publisher opts in
  via `closed_lookback` (off by default), it checks for a recently closed
  issue with the same marker (or exact legacy title) before creating a new
  one, avoiding duplicates for failures that were already fixed.

Workflow setup is centralized in `.github/actions/setup-agent`. Jobs still
check out the agent repository explicitly, then use that local composite action
to install the pinned Python toolchain, project dependencies, and optionally
the pinned Claude Code CLI. The workflow standards tests scan both workflows
and local action metadata so external actions and Claude installs do not drift.

## Repository Model

The standard model is direct upstream branches: the agent pushes
`agent/backport/...` branches to `repo` and opens PRs in that same
repository. This keeps the registry small and matches the GitHub App
permissions used by the workflows.

`push_repo` is optional and exists only as a different-owner fork escape hatch.
Same-owner `push_repo` values are rejected so staging repositories do not become
the normal deployment model.

## Test Failure Detector Flow

```text
main.py (Daily or Weekly cron, or manual dispatch; --workflow daily|weekly)
  -> get_latest_scheduled_run(workflow file) or use provided run_id
  -> source_for_run(): Daily or Weekly, from the run's workflow path
  -> Daily: download_all_test_failures() from the run's artifacts
     Weekly: no artifact (see below)
  -> job_urls_for_jobs() for CI links
  -> parse_and_deduplicate() groups by {test_name, test_file}
  -> find_unrecorded_failures() adds what only the job logs show
  -> Weekly: split_by_branch() makes one failure per release branch
  -> process_failures() creates/updates GitHub issues
```

Valkey's Weekly calls `daily.yml` once per release branch inside one run. The
run's own branch and SHA are `unstable`'s; each job name carries the branch it
tested (`run-daily-for-release-branches (8.1) / <job>`), and its log's checkout
step names the commit. Every call uploads an `all-test-failures` artifact
under the same name, each merging whichever branches' per-job artifacts
existed at that moment, so no artifact can be attributed to one branch:
Weekly failures come from job logs only (`scripts/common/valkey_ci.py`).
A Weekly failure's issue fingerprint includes its branch, so it is one issue
per release branch, apart from the Daily issue for the same test.

### Entry Points

- `scripts/test_failure_detector/main.py` - CLI entry point and pipeline orchestration
- `scripts/test_failure_detector/download.py` - workflow run discovery and artifact download
- `scripts/test_failure_detector/parse_failures.py` - JSON parsing and deduplication
- `scripts/test_failure_detector/manage_issues.py` - orchestration over the shared dedup publisher to create/update issues
- `scripts/test_failure_detector/issue_renderer.py` - test-failure-specific title/body/comment rendering and label assignment
- `scripts/common/valkey_ci.py` - Daily vs Weekly runs, a Weekly job's release branch and tested commit, and runtest result lines (shared with the CI-fix engine)

## Release Notes Flow

Supports valkey core and the module repos (valkey-search, valkey-json,
valkey-bloom). Per-repo conventions (changelog display name, version file
layout and bumper, note categories, prompt wording) are declared as
`ProjectProfile`s in `scripts/release_notes/projects.py` and resolved from the
target repo name (fork owners resolve the upstream profile).

```text
main.py (manual dispatch: repo, version, optional stage, urgency, dry_run)
  -> resolve the repo's ProjectProfile (unsupported repos fail fast)
  -> validate + canonicalize inputs; infer patch GA only when PATCH > 0
  -> dry_run selects preview (true) or PR-opening execution (false)
  -> clone the target repo (full depth + tags), validate --base-ref
  -> release_cut.cut()
       -> collect_advisory_fixes()   (if --security-from-advisories)
       -> resolve_branch_plan()      verify M.m branch exists, derive target
       -> pipeline.regenerate_unreleased()
            -> discover()  PRs over base..HEAD, deduped by PR number
            -> classify()  include, AI candidate, or no-release-notes hard exclusion
            -> PRDiffCollector  bounded, attributable commit diffs cached once
            -> triage()    completeness-first AI decision + deterministic
                            release-impact override for risky exclusions/no verdicts
            -> generate()  AI: one categorized bullet per included PR
            -> normalize operator-output categories and canonical bullet format
            -> dedup bullets by PR number (surfaces duplicate_prs)
            -> prior_notes.align_wording()  reuse a sibling release line's
                            published text for the same source PR (local, read-only)
            -> group_bullets()  {category: [canonical bullet line, ...]}
       -> _drop_already_credited()   dedup against PRs the line already ships
       -> promote_and_bump()         dated section + profile version bump + contributors
       -> _commit_push_release_pr()  prep branch (force-with-lease) + PR into the line
```

The branch model is tag-driven: all stages (rc1, rc2, ..., ga) target the existing
M.m branch (e.g. `9.1`). Maintainers create the branch and push tags before
dispatching. Tags determine the discovery range (rc1 uses the previous release tag,
rc2+ finds the prior rc tag, ga finds the last rc/patch tag). The cut lands on an
`agent/release-cut/...` prep branch and opens a PR into M.m, so the line only
advances when a human merges. The normal dispatch has five inputs and defaults to
a dry run; patch versions may omit stage and infer `ga`, while `M.m.0` always
requires an explicit stage. The advanced dispatch is a thin wrapper around the
same reusable workflow.

Each release line is cut by its own independent run, so nothing about the AI pass
makes two lines word the same backported change the same way. One carve-out
closes that gap: because `backport_refs` resolves a release-branch commit back to
its *original* source PR before the bullet's `(#N)` is written, that reference is
identical on every line and is produced by code, never by the model. Before
rendering, `prior_notes` builds an index of the sibling release lines' merged
`00-RELEASENOTES` files keyed by that number and, on a hit, substitutes the
published text for this cut's generated prose. The index is read out of refs the
clone already has (`refs/remotes/origin/<M.m>` — `main.py` clones without
`--single-branch`/`--depth` and fetches tags), so consistency costs no extra
network calls and no writes; a line whose changelog is unreadable is reported and
skipped rather than failing the cut. Only prose is carried — category, credit,
and reference remain this cut's — and only from bullets under the canonical
`### <category>` headings `release_format.render_version_section` writes, which
excludes every hand-authored legacy section. Because the shared grammar
(`trailing_pr_numbers`, the bullet form) lives in `release_format`, `prior_notes`
stays importable from `pipeline` without the `release_cut` -> `pipeline` cycle.

The prep branch name is deterministic for a version and stage. Re-dispatching
while its PR is open regenerates the full tagged range through the latest M.m
tip, rebuilds the prep commit on that tip, pushes it with force-with-lease, and
updates the existing PR. Full regeneration is intentional: it includes new
merges and also re-evaluates changed labels and PR metadata without treating a
previous AI-generated branch as durable input.

Review handling is a second, narrower flow over those generated PRs:

```text
release-notes-review-poll.yml (hourly runner; immediate + five-minute ticks)
  -> review_poll.poll_once()
       -> validate automated release PR and authorized review threads
       -> batch every actionable thread on the PR
       -> post `Addressing N release-note review comments.`
       -> dispatch release-notes-review.yml

release-notes-review.yml (serialized per repo/PR; trusted App sender only)
  -> re-fetch PR, head, status comment, threads, and author membership
  -> public clone at the exact head -> one edit-only AI pass
  -> require only the current dated notes section to change
  -> apply the patch in a clean clone
  -> re-fetch the complete batch -> commit -> normal fast-forward push
  -> update status, reply, and resolve unchanged threads
```

The poll job runs for 55 minutes. `run_poll_loop` invokes the first scan as soon
as the runner reaches the poll step, then targets five-minute intervals. PR
creation does not start a separate timer. Workflow concurrency serializes poll
runs, and the per-PR handler concurrency serializes edits.

The PR comment doubles as deduplication and recovery state through a hidden
status, head SHA, content-bound batch identifier, target thread identities, and
the proposed commit once it has been created. Addressing markers use a 90-minute
lease so a cancelled or setup-failed job cannot block the PR forever. Failed
batches are retryable; editing feedback changes the batch identity, while an
unchanged deterministic refusal remains blocked. The handler records the commit
before pushing, allowing the poller to detect an accepted push and idempotently
finish replies and resolutions after a timeout or crash without another AI
edit. Prompt and GraphQL bounds fail explicitly rather than processing a partial
batch. The publication token is never placed in the AI checkout, and the shared
AI runtime excludes it from the Claude subprocess environment.

Signals fall into two tiers. Malformed inputs, a missing target branch, an
already-released/backward target, or a target branch that advances during
generation are hard errors that abort before any PR. Warnings (out-of-sequence
stages, unresolved PRs, empty notes, security mismatches, AI-triage decisions,
deterministic triage overrides, an irreconcilable cross-line wording conflict, and
`LOW`/`MODERATE` urgency paired with
release-impact signals) hold the PR as a draft with a banner naming them. Impact
detection is a review trigger, not an automatic security or severity
classification. Re-dispatch reconciles draft state automatically. `force_ready`,
available only on the advanced dispatch, bypasses holds. An omitted release date
is explicitly the current UTC date; the advanced dispatch accepts an explicit
calendar date.

### Entry Points

- `scripts/release_notes/main.py` - CLI entry point, input validation, clone
- `.github/workflows/release-notes-cut.yml` - simple dispatch and shared reusable release job
- `.github/workflows/release-notes-cut-advanced.yml` - advanced-input wrapper around the shared job
- `scripts/release_notes/release_cut.py` - branch-plan resolution, notes rendering, PR body + `_hold_reasons` (draft-hold decision)
- `scripts/release_notes/pipeline.py` - discover -> classify -> triage -> generate -> render orchestration
- `scripts/release_notes/discover.py` - range resolution and PR discovery by graph reachability
- `scripts/release_notes/backport_refs.py` - recover the original PR of a backported commit (verified sweep Applied table, standalone backport-of URL, subject, -x trailer, branch name)
- `scripts/release_notes/classify.py` - label split: release-notes -> include, no-release-notes -> exclude, else -> triage
- `scripts/release_notes/ai_inputs.py` - shared, bounded PR prompt payloads and SHA-cached diffs; combined sweep diffs are omitted when they cannot be attributed to one source PR
- `scripts/release_notes/triage.py` - completeness-first Claude include/exclude plus deterministic release-impact guardrail for PRs without `release-notes` (no tools; PR data inlined in prompt)
- `scripts/release_notes/generate.py` - Claude bullet generation (no tools; PR data inlined in prompt)
- `scripts/release_notes/prior_notes.py` - index sibling release lines' published `00-RELEASENOTES` bullets by source PR number and reuse their wording (local git reads only; declines and holds on a cross-line contradiction)
- `scripts/release_notes/models.py` - typed dataclasses for the pipeline
- `scripts/release_notes/projects.py` - per-repo release conventions (ProjectProfile) and version bumpers (valkey version.h macros, valkey-search kModuleVersion, valkey-json CMake project VERSION, valkey-bloom Cargo.toml)
- `scripts/release_notes/security.py` - Security Fixes from published GitHub advisories (never AI-authored)
- `scripts/release_notes/render.py` - canonical `00-RELEASENOTES` rendering
- `scripts/release_notes/publish.py` - find/open/update the release PR; `_reconcile_draft` flips draft state on re-dispatch
- `scripts/release_notes/release_format.py` - `00-RELEASENOTES` dated-section rendering
- `scripts/release_notes/version_bump.py` - `src/version.h` macro rewriting
- `scripts/release_notes/contributors.py` - contributor discovery; reconciles display-name, login, co-author trailers, and PR-resolved logins
- `.github/workflows/release-notes-review-poll.yml` - discover, authorize,
  batch, mark, and dispatch unresolved inline review comments
- `.github/workflows/release-notes-review.yml` - guarded AI edit/publication job
- `scripts/release_notes/review_poll.py` - multi-repository poll orchestration
- `scripts/release_notes/review.py` - shared PR, thread, marker, and edit contracts
- `scripts/release_notes/review_handler.py` - revalidation, one AI pass, fresh-clone
  publication, replies, and resolution

## CVE Scan Flow

Verification instead of prediction. Four jobs: a deterministic `scan` that finds
candidate CVEs and reports all findings, a `verify` job that builds each candidate
image itself and scans the real artifact to prove the targeted CVEs are gone, a
`collect` job that reconciles the per-leg results and decides which version lines to
dispatch, and a `rebuild` job that dispatches valkey-container's plain build-and-publish
only on that proof. The image is built twice by design: once here to verify, once by
valkey-container to publish. That is the deliberate price of keeping their `ci.yml` a
plain build with no CVE logic.

```text
Job 1 - scan (scheduled / workflow_dispatch; needs: none, timeout 30)
sweep.py
  → image_matrix.py
      resolve_matrix() fetches versions.json once, derives image tags
  → scanner.py (Trivy subprocess per image per platform)
      scans each image on each published platform (amd64, arm64, arm/v7, ppc64le)
      deduplicates findings by (image, package, cve_id, installed_version, platform)
        (exact duplicates within a platform collapse; the same CVE on different
         platforms stays distinct)
  → classify (rebuild_decider.py): fixed_version present → candidate;
      no fixed_version → not fixable
      (a candidacy signal, NOT a prediction a rebuild will resolve it; proof is verify)
  → summary.py (render grouped findings tables for job summary)
  → emit GITHUB_OUTPUT: plan=<JSON legs {line,variant,platform,cves[]}>

Job 2 - verify (needs: scan; matrix = fromJSON(needs.scan.outputs.plan),
         max-parallel 8, fail-fast false, timeout 180; one leg per affected
         (line, variant, platform); runs only on valkey-io/valkey-ci-agent main,
         if plan is non-empty AND not dry_run; no creds)
  → checkout valkey-io/valkey-container @ mainline into ./container
  → call ./container/.github/actions/build-image, the same action ci.yml publishes with:
       explicit context ./container, dockerfile <line>/<variant>/Dockerfile,
       platforms <one>, push false, load true,
       tag cve-candidate:<line>-<variant>-<platform-slug>
  → verify_candidate.py --image-ref ... --cves-json ... --line/--variant/--platform
       records a marker with the leg outcome (verified / survivors / error);
       exit 2 (error) fails the leg loud, exit 1 (survivors) does not

Job 3 - collect (needs: [scan, verify], timeout 10)
  → collect_verification.py
      downloads uniquely named leg markers and reconciles them against the plan from scan
        (a leg with no marker = missing; an unexpected leg is rejected)
      decides dispatchable version lines (any-architecture: qualify when at least one
        affected arch is proven fixed)
  → emit GITHUB_OUTPUT: verified_versions, arch_report (per-arch proof status)

Job 4 - rebuild (needs: [scan, collect]; dispatches collect.verified_versions on
         valkey-io/valkey-ci-agent main, inside the cve-rebuild-dispatch Environment,
         when there is a dispatchable line and not dry_run; timeout 240)
  → mint Valkeyrie Bot App token (actions:write, scoped to valkey-container) - dispatch only
  → correlation_id = <github.run_id>-<github.run_attempt>
    gh workflow run ci.yml --repo valkey-io/valkey-container
         --field "version=<versions>" --field "correlation_id=<id>"
  (locate/watch/conclusion use the built-in GITHUB_TOKEN, valid for the whole job)
  → locate the triggered run by its EXACT name "CVE rebuild <id>"
       (gh run list --workflow ci.yml --branch mainline; retry; fail loud if none)
  → gh run watch <run-id> --exit-status  (wait; a failed build fails the job)
  → capture conclusion; report build result + correlation id + run URL in summary and Slack
```

The rebuild is dispatched on proof, not prediction: the `verify` job built the
candidate from the same `mainline` Dockerfiles and scanned the artifact to confirm the
targeted CVEs are gone. The dispatch gate is any-architecture: a version line is
dispatched when AT LEAST ONE affected architecture was proven fixed, not when all of
them were. valkey-container's `ci.yml` takes a single `version` input and rebuilds every
platform in one multi-platform push; it cannot target one architecture, so withholding a
dispatch because one architecture lagged would be strictly harmful (the architectures
whose fix is live stay unfixed for another week, while the lagging one is no better off).
A dispatched line may therefore still be vulnerable on an architecture whose fix has not
landed, which is why the reporting is per architecture (see below). A surviving CVE on
one architecture, or an errored or missing leg, is surfaced but does not block a line
proven fixed on another architecture; the only fail-closed no-dispatch is a run that
proved nothing at all (no architecture verified anywhere and at least one leg errored or
went missing).

The rebuild job does not stop at the dispatch: it locates the run it triggered by an
exact correlation name (`CVE rebuild <run_id>-<run_attempt>`, echoed into the run name
by valkey-container's ci.yml via the companion change's `correlation_id` input),
restricted to ci.yml on mainline, retrying for the run to appear. Exact-name
correlation replaces the old timestamp-window and actor-filter heuristics entirely. It
then waits with `gh run watch --exit-status` and reports the actual build conclusion and
run URL in the job summary and Slack. A failed downstream build therefore fails this
job, and a run that cannot be located fails loud rather than reporting an unverified
success. The Slack status is success only when the dispatch, run location, and build all
succeed; every other case (including cancelled or timed-out) normalizes to failure, so a
non-success rebuild always notifies instead of going silent. The job's `timeout-minutes`
is 240 to outlast observed full-matrix `ci.yml` builds of about 130 to 150 minutes (the
runner is billed while watching, the deliberate cost of verification).

### Entry Points

- `scripts/cve_scan/sweep.py`: orchestrates scanning, classification, summaries, and the grouped verification plan
- `scripts/cve_scan/rebuild_decider.py`: two-rule fixability classification (fixed_version present means candidate; absent means not fixable)
- `scripts/cve_scan/summary.py`: renders grouped findings tables for the job summary
- `scripts/cve_scan/verify_candidate.py`: scans a candidate for the leg's CVE IDs, independent of package names
- `scripts/cve_scan/collect_verification.py`: reconciles per-leg markers against the plan and emits verified_versions/arch_report
- `scripts/cve_scan/image_matrix.py`: resolves image tags from versions.json
- `scripts/cve_scan/scanner.py`: per-image per-platform Trivy invocation with finding deduplication

### Security Model

The design relies on proof and deterministic code:

- **Proof, not prediction (fail-closed)**: a rebuild is dispatched only after the verify job builds the candidate image itself and scans the real artifact to confirm the targeted CVEs are absent. There is no base package-database inspection, native version comparison, or install simulation; a run that verifies no architecture anywhere, with an errored or missing leg, dispatches nothing.
- **Verify our build, not the exact digest**: the verify build and valkey-container's publish build run minutes apart from the same `mainline` Dockerfiles, and OS package repositories only move forward, so a fix present in our build is present in theirs. This is strong evidence, though not digest-identical: we prove the fix on an artifact we built, not on the exact bytes valkey-container ships.
- **Targeted version dispatch**: the rebuild job passes `--field version="<versions>"` with only the affected version lines (e.g. `8.0 9.1`) rather than rebuilding all images, minimizing blast radius.
- **Every affected architecture is verified, dispatch is any-architecture**: the verify matrix emits one build per distinct `(line, variant, platform)` in the findings, so a CVE flagged on multiple architectures is proven on each. A distro can publish a package fix for one architecture before another, so per-architecture verification is what lets the report be honest. A line is dispatched when AT LEAST ONE affected architecture is proven fixed, because `ci.yml` rebuilds all platforms from a single `version` input and cannot target one architecture: a partial fix is a strict improvement (the fixed architectures ship, the lagging one is rebuilt no worse off). A dispatched line may still be vulnerable on an architecture whose fix has not landed, so the `collect` job emits a per-architecture `arch_report` and the summary and Slack state which architectures were proven fixed and which remain vulnerable, never implying a blanket fix. The cost: up to fourfold more verification builds per line and variant (four published platforms), two of them (`linux/arm/v7`, `linux/ppc64le`) QEMU-emulated source compiles.
- **One canonical build definition**: valkey-container owns `.github/actions/build-image`, and both its publishing workflow and the agent verification job call it. The agent supplies an explicit path context and local-only push/load settings, while the action owns QEMU, Buildx, Dockerfile handling, and provenance.
- **No credentials in verify**: the verify job builds with `push: false` / `load: true` and holds no registry credentials, so the candidate never leaves the runner.
- **Deterministic code path**: scanning, classification, candidate build, verification, and dispatch are all deterministic code with no AI in the loop.
- **Least-privilege tokens**: the scan, verify, and collect jobs need only `contents:read`. The rebuild job mints a separate App token scoped to `actions:write` + `metadata:read` for the dispatch step only; the run-tracking steps use the built-in `GITHUB_TOKEN`.
- **Audit trail**: every automatic dispatch is recorded in the workflow run log and the job summary, including the correlation id and the downstream run URL.

This matches valkey-container's existing posture: the same `ci.yml` workflow already runs automatically on cron and on push. The CVE scanner dispatches it through the same path with the same effect, triggered by verified vulnerability evidence rather than a timer.

### Authentication

The rebuild job authenticates as the **Valkeyrie Bot GitHub App** by minting a short-lived, repo-scoped installation token (via `actions/create-github-app-token`) with `actions:write` scope, used only by the dispatch step. The locate, watch, and conclusion steps instead use the built-in `GITHUB_TOKEN`, because App installation tokens expire after one hour while the watched build runs 130 to 150 minutes; valkey-container is public, so `GITHUB_TOKEN` can read its Actions runs and no privileged App credential is held for the full wait. The scan, verify, and collect jobs require only `contents:read` (the verify job holds no credentials at all). The rebuild job is restricted to `valkey-io/valkey-ci-agent` on `refs/heads/main` and runs inside the `cve-rebuild-dispatch` protected Environment, a credential boundary (not an approval gate) that scopes the App credentials and dispatch permission to `main`. There is no PAT fallback: forks are scan/dry-run only and cannot dispatch.

### Idempotency

Concurrency is declared per job. The scan and verify jobs cancel/redo cheaply when
superseded; the rebuild job uses a separate group with `cancel-in-progress: false` so an
in-flight rebuild watcher is never cancelled while the container build continues.

### Configuration

Settings are loaded from `CVE_SCAN_*` environment variables with sensible defaults
targeting `valkey-io/valkey-container`. The workflow pins all values explicitly in
its `env:` block (house style: visible-in-workflow configuration, matching the
`CI_FIX_*` and `RELEASE_NOTES_*` patterns used by other workflows). Invalid values
raise immediately rather than acting on defaults.

## Planned Workflows

Future sibling modules and extensions:

- **PR Reviewer** - two-stage code review with skeptic pass
- **Flaky test detection** - detect flaky tests across Daily runs


## Release automation

Release automation is a short pipeline whose durable identities are the
preparation PR and the exact release-branch commit:

```
Prepare Release (this repository's Actions UI, dispatched by a maintainer)
  -> authorize the dispatching maintainer against the authorized teams (valkey-committers or valkey-release) live
  -> derive version from branch + tags
  -> create/update non-authoritative Release <tag> tracking issue
  -> open/update agent/release-cut/<version>-<stage> PR
  -> maintainer review and merge

Refresh Release Progress (one hourly runner; five-minute polls for 55 minutes)
  -> derive status from live PR, branch, workflow, and release APIs
  -> if canonical preparation PR merged and merge SHA is still branch HEAD:
       dispatch Publish Release <branch, merge SHA> once
  -> update the tracking issue with failure and next action

Publish Release <branch, candidate SHA>
  -> require candidate == canonical preparation PR merge commit
  -> require candidate == current branch head
  -> validate version.h, release notes, tag availability, and report ci.yml status
  -> call the secretless no-publish qualification workflow synchronously
  -> protected release environment approval
  -> live authorized-team authorization of the recorded approver
  -> recompute and compare the complete plan digest
  -> atomically create tag at candidate SHA
  -> publish GitHub release
  -> existing Valkey release event starts production automation
       -> archives + RPM/DEB + hashes + container + docs + website + Try Valkey
       -> wait for exact public container images, then update Bundle
       -> draft Helm bump for GA; make ready after its image is public
  -> for X.Y.0, open the bounded backport-registry onboarding issue/PR
```

There is no controller-owned state. The bot-owned status comment holds the
tracker identity; the issue body is a projection for humans. Deleting or editing
the body cannot authorize publication, select a SHA, or
skip a gate. A moved branch, failed check, or failed qualification is recovered
by fixing the repository state and rerunning the appropriate workflow. GitHub
Actions supplies execution history; the canonical release preparation PR
supplies review history; the exact SHA binds both.

The asynchronous container-to-chart ordering is kept explicit: production
automation opens the Helm change as a draft, and a maintainer makes it ready
after the public image exists. This keeps the Helm output without requiring
durable receipts or a cross-repository state machine. The progress watcher is
intentionally narrower: one hourly runner remains alive for 55 minutes and
polls every five minutes, observes live state, performs the one idempotent
notes-merge-to-publication dispatch, and refreshes the issue. The bounded loop
bridges delayed cron starts without outliving its short-lived App tokens.

The control App is available only through the default-branch-restricted
`release-control` environment and is not a release-tag ruleset bypass actor.
The publication App is separate and exists only in the reviewer-protected
`release` environment. The production automation repository retains its
`release-publish` approval before public uploads and downstream writes, giving
the flow two deployment approvals without a relay App, HMAC, or shared secret.
Releases start from this repository's own Actions UI: Prepare Release
authorizes the dispatching maintainer live against `valkey-committers` or
`valkey-release`, so there is no
relay workflow, no forwarded-identity input, and no cross-repository
Actions-write grant.
