# valkey-ci-agent

An AI-powered CI automation agent for the Valkey project. Uses Claude Code (Anthropic Claude Fable 5 via Bedrock) to perform tasks that require code understanding - conflict resolution, code review, failure analysis, and more.

## Architecture

The agent is structured as a layered framework:

```text
scripts/
  ai/          AI layer: Claude Code subprocess orchestration
  backport/    Automated backports (active)
  fuzzer/      Fuzzer run monitoring (active)
  ci_fix/      CI failure triage and fix (active)
  release_notes/  Release cutter + guarded review-comment edits (active)
  cve_scan/    CVE scanning + verified rebuild dispatch (build-and-prove, active)
  common/      Shared infrastructure (git auth, GitHub client, safety guards)
.github/actions/setup-agent
              Shared workflow setup for Python deps and optional Claude Code
.github/actions/backport-sweep
              The prepare-then-publish sweep steps the daily sweep and poller share
repos.yml      Central registry of repos, branches, and project boards
```

New workflows are added as sibling directories to `backport/`. Each workflow picks an agent profile (tools, timeout, effort) and writes its own prompt. The AI layer and shared infra stay unchanged.

**Workflows:**

| Workflow | Status | Description |
|----------|--------|-------------|
| Backport | Active | Cherry-picks merged PRs onto release branches with AI conflict resolution |
| Fuzzer Monitor | Active | Analyzes scheduled fuzzer runs and files issues for anomalous failures |
| CI Fix | Manual trial | Diagnoses one exact failed job and prepares a fix using the existing backport App/OIDC and normal-PR-CI model |
| Test Failure Detector | Active | Detects test failures from Daily and Weekly CI, files/updates GitHub issues |
| Release Notes | Active | Cuts a release for valkey core or a module repo (search/json/bloom): AI-generates notes from `release-notes` PRs plus AI-triaged candidates without that label, promotes them onto a release line branch, bumps the repo's version file, opens a PR (held as a draft when the cut flags issues) |
| Release Note Review | Active | Polls unresolved inline comments on automated release PRs, applies a guarded AI edit to the current dated section, replies, and resolves addressed threads |
| CVE Scan | Active | Scans container images for vulnerabilities, builds and scans the candidate image itself to prove the fix, then dispatches a targeted rebuild |
| PR Reviewer | Planned | Two-stage code review with skeptic pass |
| Flaky Test Detection | Planned | Detects flaky tests across Daily runs |

## Backport Workflow

The currently active workflow. Cherry-picks merged PRs onto release branches with AI-powered conflict resolution. Works for any repo defined in `repos.yml` - Valkey core, Valkey modules (bloom, search, json), or anything else following the per-branch project-board pattern.

### How it works

1. **Daily sweep** - every day at 09:00 UTC, the preflight job reads `repos.yml` and generates one matrix leg per `{repo, branch}` pair
2. **Project discovery** - each leg queries the GitHub Project v2 board for PRs marked "To be backported"
3. **Cherry-pick** - attempts `git cherry-pick` for each candidate onto the target release branch
4. **AI conflict resolution** - when cherry-pick conflicts, Claude Code reads both sides and resolves the conflict in place
5. **Validation** - registry build/rule commands plus an optional repository profile run against each candidate's own diff; any failure removes that candidate
6. **PR creation** - pushes the branch and opens (or updates) a PR with a summary table
7. **CI follow-up** - enabled repositories inspect the current head of every open bot-owned PR into a registered branch (the sweep PR, a pushed CI fix, a draft fix PR) and give one unhandled actionable failure to the shared CI-fix engine
8. **Status sync** - after a backport PR is merged into the release branch, the source PR's Project v2 status can be moved from "To be backported" to "Done"

Manual single-PR backports are also supported via `workflow_dispatch`.

A daily eligibility pass (`backport-eligibility.yml`) comments on PRs merged to the development branch that look worth backporting, naming the release branches that carry the changed code. It only suggests; maintainers still add the PR to the project boards.

### Registry (`repos.yml`)

The registry is the single source of truth. To onboard a new repo, add an entry to `repos.yml`:

```yaml
repos:
  - repo: valkey-io/valkey
    project_owner: valkey-io
    project_owner_type: organization
    language: c                          # used in conflict resolver prompt
    validation_setup_commands:
      - "./ci/setup-backport-validation.sh" # optional; run once in clone
    build_commands:
      - "make -j$(nproc)"                # run before push; empty = skip
    validation_profile: valkey-core       # optional dynamic, diff-aware checks
    validation_rules:                     # optional subsystem checks by path
      - paths: ["src/rdb.c"]
        commands: ["./runtest --single unit/dump --clients 1"]
    generated_file_rules:                 # optional deterministic regeneration
      - paths: ["src/unit/*.c"]
        command: "python3 utils/generate-unit-test-header.py"
        outputs: ["src/unit/test_files.h"]
    repair_validation_failures: false    # optional; one AI repair attempt on failure
    automatic_ci_followup: false         # inspect CI on open bot-owned PRs
    ci_followup_ignored_jobs: ["*dco*"]  # informational failures, never fixed
    backport_label: backport
    llm_conflict_label: ai-resolved-conflicts
    max_conflicting_files: 100
    branches:
      - branch: "8.1"
        project_number: 14
      - branch: "9.0"
        project_number: 18
```

By default, agent branches are pushed directly to `repo` under the `agent/backport/...` namespace and PRs are opened in that same upstream repository. `push_repo` is optional and only exists as an escape hatch for a real different-owner fork; same-owner `push_repo` values are rejected so staging repositories do not become the normal model.

The sweep branch is always kept green: a candidate is only kept if the whole branch still validates after the cherry-pick, so one bad commit can never block later candidates. Each scheduled run keeps up to two validated cherry-picks (`--max-candidates 2`) and reports candidates that were skipped or failed validation in the PR's "Needs attention" section without committing them. When `repair_validation_failures` is enabled, Claude Code gets one narrow edit-only attempt to fix a failing cherry-pick before it is dropped. A test file that the target branch cannot build or run is never silently discarded: the agent must port its intent into an existing branch-native test and revalidate the full candidate, or the candidate fails closed. This adaptation preserves regression coverage that a fail-closed-only policy would discard when release branches use different test harnesses. Added files belonging only to that unsupported test harness are supplied as adaptation context and removed in the same validated commit; independently mapped tests remain unchanged.

The `valkey-core` profile adds checks that static globs cannot express safely:
`git diff --check` over the candidate range, branch-native `clang-format-18`
over only candidate line ranges and suffixes enforced by that release's
workflow, directly changed Tcl tests (including required modes such as TLS),
C/C++ unit tests, branch-compatible checks for cluster, sentinel, module, or
test-harness changes, and targeted reply-schema validation for changed tests
that do not opt out. It checks the target branch's actual C/C++ unit-test
discovery convention, so a clean cross-harness cherry-pick cannot pass while
its new test file is ignored.
Generated-file
rules run twice to prove convergence. They may edit only their declared tracked
outputs; an allowed update is amended into the candidate commit, while any
unexpected path or non-convergent generator fails closed.

When `automatic_ci_followup` is enabled, `backport-ci-followup.yml` runs only
for registered branches with an App-owned open PR whose head branch is one this
engine writes: the `agent/backport/sweep/<target>` sweep PR first, then any
`backport/`, `agent/backport/` or `agent/ci-fix/` PR into that branch, so a
pushed CI fix and a draft fix PR get another round when their own CI goes red.
A backport head is diagnosed under the backport policy (its failure is usually a
missing upstream change) and any other head under the general one. It requires
the exact base, head repository, branch and 40-character current SHA, waits for all current-head runs to finish,
ignores the Actions jobs named in `ci_followup_ignored_jobs`, and records one
attempt per logical workflow job in hidden PR-comment markers, deduplicating
twin push and pull-request runs for the same head. A current head receives at
most three follow-up attempts. Only Actions jobs are ever candidates, so checks
contributed by other GitHub Apps -- DCO and Codecov on Valkey core -- are out
of scope without any configuration. Sweep publication, poll, and CI follow-up
share one per-repository/branch concurrency group. Each invocation selects one
exact failed job, gives its log and the bound head to one diagnose-and-edit
agent session, then pushes a lease-protected descendant commit. The target PR's
normal CI and human review decide whether that commit is acceptable. Refusals
are reported without changing the branch.
The bot never adds DCO sign-off and never rewrites a published sweep branch.
During the initial rollout the workflow has no schedule; dispatch it manually
to exercise this path on an existing sweep PR.

See [`examples/repos.yml`](examples/repos.yml) for a multi-module example.

### Setup and Usage

See [DEVELOPMENT.md](DEVELOPMENT.md) for local setup, local validation commands,
required GitHub Actions secrets, and manual workflow dispatch examples.

## Fuzzer Monitor Workflow

The fuzzer monitor watches scheduled `valkey-io/valkey-fuzzer` workflow runs, analyzes their artifacts, and files issues for runs that look anomalous.

### How it works

1. **Cron** - every 4 hours, the monitor checks the latest scheduled fuzzer run
2. **Deterministic scan** - pattern-matches crash/sanitizer/failover/RDB signals against artifact JSON and node logs; ignores chaos-expected noise (CLUSTERDOWN, replication link loss)
3. **Claude Code analysis** - drops the artifacts in a tempdir, shallow-clones `valkey-io/valkey` at the tested commit and `valkey-io/valkey-fuzzer` at the run's HEAD, then asks Claude (with read-only `Read,Grep,Glob` tools) to correlate the failure with source and decide whether the run reflects a real bug or chaos-expected noise. If a clone fails the prompt tells Claude not to cite source line numbers.
4. **Issue upsert** - anomalous runs file (or update) an issue on `valkey-io/valkey-fuzzer`, deduplicated by a stable fingerprint over root cause and anomaly shape
5. **Audit** - per-run JSON results and Claude evidence are uploaded as workflow artifacts

The Claude Code subprocess runs under the `fuzzer_analysis_readonly` agent profile with `Read,Grep,Glob` tools only - no editing, no Bash, no network access beyond the Bedrock call itself.

### Configuration

The monitor reuses the same secrets and OIDC role as the backport workflow (see [Step 1](#step-1-configure-secrets-and-variables) above). The Valkeyrie GitHub App needs `actions:read`, `contents:read`, and `issues:write` on `valkey-io/valkey-fuzzer`; the workflow mints a short-lived installation token scoped to that repository only.

### Manual run

```bash
# Run live against the latest scheduled fuzzer run (default)
gh workflow run monitor-fuzzer.yml --repo valkey-io/valkey-ci-agent

# Probe without invoking Claude or filing issues
gh workflow run monitor-fuzzer.yml \
  --repo valkey-io/valkey-ci-agent \
  --field dry_run=true
```

Scheduled runs always run live.

## Test Failure Detector

Monitors the Daily and Weekly CI workflows on `valkey-io/valkey`, detects test failures, and automatically creates or updates GitHub issues to track them.

Weekly (`weekly.yml`) runs `daily.yml` once per release branch (7.2 and newer) inside one run, so it is read differently:

- every job name carries the branch it tested: `run-daily-for-release-branches (8.1) / test-valgrind-test (unit)`;
- each branch uploads its own `all-test-failures` artifact under the same name, each mixing the branches' jobs, so Weekly failures are read from job logs only;
- a Weekly failure gets one issue per release branch (`[TEST-FAILURE] <test> in <file> (8.1)`, with a `Release branch` line in the body), separate from the Daily issue for the same test, because each needs its own fix on its branch.

### How it works

The detector is a thin pipeline (`scripts/test_failure_detector/`) layered on shared building blocks in `scripts/common/` - `ArtifactClient` for artifact download and `IssueDedupPublisher` for issue dedup/publishing - so the same primitives back the Fuzzer Monitor.

1. **Daily sweep** - every day at 23:00 UTC the workflow runs on `valkey-io/valkey-ci-agent` and reads from `valkey-io/valkey`. Valkey Daily CI runs daily at 00:00 UTC, so this detector workflow will capture the current day's run. In case of a miss for any reason, manual dispatch functionality is available (more detail in 'Scheduled' section below).
2. **Find the run** - locates the most recent completed, non-cancelled/skipped Daily workflow run on the target branch (`unstable` by default), or uses a manually supplied run ID
3. **Download artifact** - `ArtifactClient` fetches the `all-test-failures` artifact, handling the auth-header-stripping redirect to Azure blob storage, transient-failure retries, and expired-artifact (404) cases
4. **Get job URLs** - fetches job metadata from the run to build CI links for each failure, with normalized name variants for fuzzy matching against artifact names
5. **Parse and deduplicate** - iterates the nested JSON (`{job -> suite -> [failures]}`) and groups by a `{test_name}::{test_file}` fingerprint, so a test failing across multiple jobs becomes one `UniqueFailure` with multiple job references
6. **Create or update issues** - `IssueDedupPublisher` upserts one issue per fingerprint, matching on a hidden body marker (`<!-- valkey-ci-agent:test-failure:<hex-hash> -->`, where the hash is derived from the test name and file) rather than the title. Per-failure rendering (title, body, recurrence comment, `test-failure` label) lives in `issue_renderer.py`. Each failure resolves to one of four outcomes:
   - **created** - no matching issue exists: opens one titled `[TEST-FAILURE] {test_name} in {test_file}` with the `test-failure` label, error trace, CI links, and environment list
   - **updated** - a matching issue exists: merges any new failing environments into the body and bumps the occurrence counter / adds a recurrence comment
   - **skipped** - the run ID matches the `last-key` marker already recorded on the issue, so a re-triggered sweep over the same CI run does not inflate the occurrence count or post a duplicate comment
   - **skipped-recently-closed** - no open issue matches, but a matching issue (by marker, or by exact title for issues from the older fingerprint scheme) was closed within the past day; creation is suppressed because the failure was likely already fixed. This check is opt-in on the shared publisher and enabled only by the detector, so the fuzzer monitor never suppresses a recurring incident

A GitHub Actions job summary is emitted at every exit path with a table of metrics (failures detected, issues created/updated).

#### Prerequisites: Cross-repo Authentication

The workflow generates a GitHub App installation token scoped to the `valkey-io` org using the same App secrets as the backport workflow (`VALKEYRIE_BOT_APP_ID` + `VALKEYRIE_BOT_PRIVATE_KEY`). This token provides `actions:read` (to download artifacts) and `issues:write` (to create/update issues) on `valkey-io/valkey`.

### Usage

#### Scheduled (automatic)

Weekly is analyzed on Mondays at 12:00 UTC (`--workflow weekly`); Valkey's Weekly starts Sunday at 06:00 UTC and has finished as late as 01:42 UTC Monday. A Weekly run older than 7 days is never read, so a run still in progress is skipped rather than replaced by last week's. A run given by `run_id` is read as the workflow it ran (manual dispatches included); a run of any other workflow is refused. Issues filed from a manually started run are not fixed automatically: `ci-fix-issues.yml` takes only scheduled runs. Daily is analyzed as follows.

Runs daily at 23:00 UTC via cron. The workflow runs on `valkey-io/valkey-ci-agent` and uses a GitHub App token to read artifacts from the most recent completed, non-cancelled/skipped Daily workflow run on the target branch (`unstable` by default), and write issues to `valkey-io/valkey`. Valkey Daily CI runs daily at 00:00 UTC, with runs typically completing within 4-7 hours, with slight exception (from valkey-io/valkey's history of 411 completed runs, 6 runs exceed 7 hours, with the longest lasting 10h 02m), such that Test Failure Detector should always capture the current day's workflow (workflow completes within seconds). In the event of a missed run, the current detector includes manual dispatch functionality, targeting a given run ID. Manual dispatches can be performed so long as Daily CI artifacts persist, currently set at 30 days.

#### Manual dispatch

```bash
gh workflow run test-failure-detector-sweep.yml \
  --repo valkey-io/valkey-ci-agent \
  --field workflow=daily \
  --field repo=valkey-io/valkey \
  --field run_id=12345678 \
  --field dry_run=true
```

- `workflow` - `daily` or `weekly` (a run given by `run_id` is identified by its own workflow file)
- `repo` - target repository to scan (default: `valkey-io/valkey`)
- `run_id` - specific workflow run ID to analyze (empty = latest run of the workflow)
- `dry_run` - parse and report only, don't create/update issues

## CI Fix Workflow

The CI-fix bot follows the same operating model as the backport workflows:
prepare one change on a bot branch, let the target PR run its normal CI, and
leave the merge decision to a human. Before publishing, it tests and reviews
its own patch in a feedback loop (below), inside the very CI job that failed
when it can, and the PR's CI remains the authority.

One compact engine sits behind three manual-first front doors:

- **Maintainer request** - an active member of `valkey-io/contributors` can put
  the following command at the start of a `valkey-io/valkey` PR comment:

  ```text
  @valkeyrie-ops fix [<run-or-job-url>] [hint]
  ```

  Manually dispatch `ci-fix-comment-poll.yml` to scan recent comments once and
  dispatch `ci-fix.yml`; the poller only authorizes, claims, and forwards the
  command. A run or job link selects that exact failure. Without a link, the
  gate chooses the most actionable completed failure on the current PR head.
- **Bot-owned PR CI** - manually dispatch `backport-ci-followup.yml` for a
  repository with `automatic_ci_followup` enabled. It selects one unhandled
  failed Actions job from the current head of an open bot-owned PR: the sweep
  PR, a CI fix it pushed, or a draft fix PR it opened.
- **Detector issue** - manually dispatch `ci-fix-issues.yml` with one Test
  Failure Detector issue number, from Daily or Weekly, or let
  `ci-fix-issue-dispatch.yml` start one run per open Daily issue. See
  [Detector issue fixes](#detector-issue-fixes).

`ci-fix.yml` can also be dispatched by hand:

```bash
gh workflow run ci-fix.yml \
  --repo valkey-io/valkey-ci-agent \
  --field repo=valkey-io/valkey \
  --field pr=<pr-number> \
  --field run_url=https://github.com/valkey-io/valkey/actions/runs/<run_id>
```

The bot fixes one failure per invocation. Every front door runs it the same
way, as three jobs of `ci-fix-run.yml` (`scripts/ci_fix/run.py`):

```text
plan -> fix-in-copy | fix-local -> publish
```

- `plan` binds the request to one exact failed job (step 1 below) and hands
  it, with that job's log, to the next jobs as an artifact.
- `fix-in-copy` is the failed job itself, from this repository's copy of its
  workflow ([Valkey workflow copies](#valkey-workflow-copies)): its own
  setup and build run, and then steps 3 to 6 run in that checkout,
  with the job's own build and test commands. A failure no copy can host (a
  VM job, a job the copies lack, another repository's, or a suggestion,
  which is never run) goes through the same steps on the bot's own Ubuntu
  runner (`fix-local`). Neither holds a GitHub token.
- `publish` does step 7 and reports the result where the request came from,
  also when the fix job failed or never finished.

### How it works

1. **Bind one task** - the front door records the repository, run attempt,
   exact failed job ID and name, tested SHA, and publication target. PR
   requests require the run to match the open PR's current head. Only a
   same-repository, bot-authored PR under `backport/...`, `agent/backport/...`,
   or `agent/ci-fix/...` is writable; every contributor or fork PR is a
   suggestion-only target.
2. **Load exact evidence** - code loads one immutable job snapshot for the
   selected run attempt, derives job selection and detector metadata from that
   same snapshot, downloads the selected job's log rather than an aggregate
   run log, and checks out the commit on which the candidate will be prepared
   (the copied job's own checkout, or a clone).
3. **Look for an existing fix** - before anything else, code searches the
   checkout's history (every branch is there) for merged commits that name
   the failing test, change its file or a source file the log names (as a
   compile error does), or reference its issue. On a release
   branch a match on the default branch is a port candidate unless it was
   already cherry-picked; on the default branch a match after the failing
   run means it may already be fixed. Candidates and their diffs go to the
   agent; they are evidence, not a decision.
4. **Reproduce** - for a test in Valkey's Tcl suite, code builds the way the
   failed job did (in its copied job, with that job's own `make` and `runtest`
   commands; elsewhere, plain `make` and `runtest`) and runs only the failing
   tests,
   `./runtest --single <unit> --only '<test>' --loops N --fastfail`, on the
   unfixed code: once (twice if it fails, to tell a frequent flake from a
   deterministic failure), then 20 loops (2 under valgrind), then the same
   loops with every core busy. A file that uses `run_solo` loops with one
   test client, since runtest runs `run_solo` inline under `--loops`.
   A run counts only if the output shows that test failing (or a valgrind or
   sanitizer report); a build break or a harness exception does not. Branches
   whose `runtest` lacks `--fastfail` (7.2) run every loop to the end.
5. **Diagnose and edit** - one Claude Code session, told what steps 3 and 4
   found and given Valkey's test conventions
   ([scripts/ci_fix/guides/valkey-io__valkey.md](scripts/ci_fix/guides/valkey-io__valkey.md)
   plus the repository's own `tests/README.md` and contributor docs), chooses
   one structured action: port a merged fix (preferred when one fits), make a
   minimal patch, or refuse. The session may edit its disposable checkout but
   cannot commit or push.
6. **Package the candidate** - code rejects agent-created commits, undeclared
   or mismatched changed paths, incomplete diagnoses, malformed port SHAs, and
   oversized suggestions. A port is independently checked to be merged on the
   default branch and absent from the target head.
7. **Publish ordinarily** - for a bot PR, code reapplies the patch or
   cherry-picks the port in a clean clone and uses an exact-head
   `--force-with-lease`. For any contributor or fork PR, it posts the concrete
   patch without changing the branch. For a detector issue, it creates an
   `agent/ci-fix/...` branch and opens a draft PR against the branch the
   failing run tested. In every case, normal target PR CI and human review are
   authoritative; the bot never merges.

Before a patch or port for a bot PR or a detector issue is published, it goes
through a feedback loop of up to three rounds, within a 60-minute budget (of
which reproduction may use 20):

1. A port must be merged on the default branch and is cherry-picked into the
   checkout; one that conflicts goes back to the agent, which may adapt it as
   a patch.
2. With the candidate applied, code runs the reproduction command again,
   looped, and under load if that is how it reproduced: 10 times for a
   failure seen on both plain runs, 40 for an intermittent one (4 either way
   under valgrind). When code's reproduction never failed, or there was none, the
   agent's own test command, if it gives one, must fail without the patch
   (it is tried up to twice, or ten times for a flaky failure) and pass with
   it as many times.
3. A separate read-only review session, given the same conventions and the
   reproduction result, must approve it. The agent's time in each round
   leaves 10 minutes for this step and the previous one.
4. When any of these does not hold, the test output or the review's reasoning
   goes back to the agent, which revises its edits, and the round repeats.

A failure that did not reproduce can still be fixed from the log, but the
report says the fix is unproven. A candidate that does not pass within the
rounds is refused. A suggestion for a contributor or fork PR is reviewed but
never run. The fix job holds no GitHub token, and every CI-fix agent (edit,
review, suggestion) and every build and test command runs as a separate
`ci-fix-agent` account, which cannot read the job's model credentials. That
account owns the checkout while it runs, so code refuses to continue if `.git`
was replaced, and before verifying removes every ignored file the job's own
setup did not leave as it is (a planted prebuilt binary). Valkey's build and
test dependencies (Tcl, `tcl-tls`, valgrind, OpenSSL) are installed before any
credential exists. A change under `.github/` is never
pushed, and the tokens carry no `workflows` scope. The PR's own CI and human
review stay authoritative; what the bot checked, including the existing-fix
search and the reproduction, is listed in its comment.

### Valkey workflow copies

To fix a failure in the environment that failed (its runner, container,
setup, build flags and test command), this repository keeps a callable copy
of Valkey's `daily.yml` and `ci.yml` for `unstable` and every release branch
from 7.2 on: `.github/workflows/valkey-copy-<branch>-<workflow>.yml`, written
by `scripts/valkey_workflows/sync.py`.

A copy holds the jobs the agent can run in: an Ubuntu or macOS runner, or a
container on an Ubuntu runner, but no VM; one `make` step and one `runtest`
command. It runs exactly one of them, named by its `job` input, against
`use_repo`/`use_git_ref`: the job's own steps up to its tests, and then,
instead of the tests, the CI-fix agent prepares the fix `ci-fix-run.yml`
planned under the name `fix`, in the job's own checkout
(`scripts/ci_fix/job.sh`), rebuilding with the job's `make` command and
testing with its `runtest` line. A matrix job runs in the one entry
`fix_matrix` names. The copies receive only `contents: read`, the OIDC token
and `AWS_ROLE_ARN` for the model, and no GitHub token that can write.

`valkey-copy-job.yml` routes a call to the copy for a branch and workflow, and
`scripts/valkey_workflows/manifest.json` maps each copy's job names to job
ids. `valkey-workflows-sync.yml` rewrites them every Monday and opens a pull
request when Valkey's workflows changed; the pull request names every job
that lost its copy.

### Configuration

The three fix workflows reuse the backport secrets and OIDC role (see
[DEVELOPMENT.md](DEVELOPMENT.md#required-secrets-and-variables)). The `plan`
and `publish` jobs each generate a short-lived, repository-scoped App token
for the reads, comments, bot-branch writes, and draft-PR creation the front
door needs; the fix job, which runs the agent, holds the AWS OIDC session
alone. Claude Code does not receive GitHub credentials;
code owns commits and pushes.

`ci-fix.yml`, `ci-fix-comment-poll.yml`, `backport-ci-followup.yml`, and
`ci-fix-issues.yml` are all manual-only during the trial.
`ci-fix-issue-dispatch.yml`'s hourly schedule does nothing until
`CI_FIX_ISSUES_AUTO_DISPATCH` is `true`.

| Variable | Default | Purpose |
|---|---|---|
| `CI_FIX_AGENT_USER` | set by the workflow | Account the agent and its test commands run as; required inside Actions |

### Detector issue fixes

`ci-fix-issues.yml` requires an explicit `issue=<N>` on every dispatch, and
one run handles one issue. `ci-fix-issue-dispatch.yml` picks the issues: on
each tick it reads the newest open detector issues in `valkey-io/valkey` and
dispatches a run for each one that has none queued or running, is unassigned,
has no open linked PR, and has no bot result comment for the occurrence it
records yet (a newer occurrence makes it eligible again). Weekly issues wait
for the Weekly flow. At most `max_runs` (`CI_FIX_ISSUES_MAX_RUNS`, default 2)
`ci-fix-issues.yml` runs are active at once, counting the running ones, which
it finds by their run name. Manual dispatches default to a dry run that only
lists what it would start. The front door requires an open,
unassigned detector-authored issue with no open linked PR, parses its latest
run marker, verifies that it names a scheduled `daily.yml` or
`weekly.yml` run, and resolves the recorded failure to one exact failed job.

The shared engine then prepares the candidate on the current tip of the
branch that run tested: `unstable` for Daily, and for Weekly the release
branch in the failing job's name (its tested commit is read from the job
log's checkout step). It keeps the tested SHA and exact job log as evidence.
If it finds a candidate, code creates a unique
`agent/ci-fix/issue-<issue>-<run>` branch and immediately opens a
draft PR against that branch, containing `Fixes #<issue>` (for Weekly,
`Refs #<issue>` and a note to close it: GitHub closes issues only from merges
into the default branch). One test file often holds several open failures, so
when the patch changes the test file of another open detector issue, that
issue's own test is reproduced and verified separately: it must fail without the
patch and pass with it before the PR says `Fixes` it. An issue whose test the
patch changed without that being shown is named in the body with no `Fixes`
line. The issue receives
the terminal result, and the new PR's normal CI and human review decide
whether the change is mergeable.

The fix runs like every other (`plan -> fix-in-copy | fix-local -> publish`,
above), in the copied job on the branch tip: Daily and Weekly both ran
`unstable`'s `daily.yml`, Weekly against the release branch. `publish`
rechecks the issue right before it opens the draft PR, and reports on the
issue, also when the fix job failed or never finished.

## Release Notes Workflow

Cuts a release in one shot for valkey core or a module repo (`valkey-search`,
`valkey-json`, `valkey-bloom`). A maintainer dispatches the target repo, version,
and urgency, plus an explicit stage for `.0` releases; patch versions infer `ga`.
The agent derives the M.m release line and generates notes from the `release-notes`
PRs plus candidates without that label that AI triage judges user-facing (Claude
via Bedrock). Deterministic release-impact checks keep crash, memory-safety,
corruption, access-control, protocol, compatibility, and similar fixes from being
silently excluded by an AI verdict. The agent renders the result onto the
long-running release line as a dated section, bumps the repo's version file,
refreshes the running contributor list, and opens one PR for review (as a draft,
holding the merge, when the cut flags anything a maintainer should address first;
see [Edge-case handling](#edge-case-handling)).
Nothing accumulates notes on a branch; the notes for a release are generated all
at once. The release line is changed only when a maintainer merges the generated
PR.

Per-repo conventions (changelog heading name, version file layout, note
categories, prompt wording) live in `scripts/release_notes/projects.py`:

| Repo | Version file | Stage recorded |
|---|---|---|
| valkey | `src/version.h` (`VALKEY_VERSION` macros) | yes |
| valkey-search | `src/version.h` (`kModuleVersion` + `MODULE_RELEASE_STAGE`) | yes |
| valkey-json | `CMakeLists.txt` (`project(... VERSION M.m.p)`) | no |
| valkey-bloom | `Cargo.toml` (`[package] version`) | no |

The release-control GitHub App installation must include every repository that can be
selected: `valkey`, `valkey-search`, `valkey-json`, and `valkey-bloom` (or choose
**All repositories**). Each workflow token is scoped to only the selected
`${{ inputs.repo }}` and requests `contents:write`, `pull-requests:write`, and
`metadata:read`. Advisory-backed cuts additionally require
`repository-advisories:read`; normal cuts do not. Token creation fails when the
installation does not include the selected repository or lacks a requested
permission.

For repos whose version file records no stage, tag-based validation remains the
authoritative check against re-cutting an already-tagged stage. The
valkey-search 1.0 line (version inline in `src/module_loader.cc`) is not
supported and fails with a clear error.

Before their first automated cut, all three module repositories (`valkey-search`,
`valkey-json`, and `valkey-bloom`) need a one-time `00-RELEASENOTES`
normalization. The trailing historical contributor block must become the
canonical cumulative footer used by this workflow: a `### Contributors` heading
followed by one `* Display Name @handle` entry per line. Matching legacy text
inside a dated release section may remain because the renderer carries dated
history forward verbatim; only the trailing footer needs normalization. Legacy
repository-specific footer formats are not carried as permanent parsing rules
here.

Unstable sentinels are repository-specific: valkey core uses
`255.255.255-dev`, valkey-json uses numeric `99.99.99`, and valkey-bloom uses
`99.99.99-dev`. The selected version bumper recognizes only its repository's
sentinel and replaces it with the requested version when a new release line is
cut.

The normal dispatch defaults to a read-only preview. For rc1 of a new minor line:

```bash
gh workflow run release-notes-cut.yml \
  --repo valkey-io/valkey-ci-agent \
  --field repo=valkey \
  --field version=9.1.0 \
  --field stage=rc1 \
  --field urgency=LOW
```

The target branch (`9.1`) and baseline are derived automatically. Other `.0`
stages differ only in `stage`; a patch release leaves it empty:

```bash
# Next RC (the 9.1.0-rc1 tag must exist on the 9.1 branch)
gh workflow run release-notes-cut.yml --repo valkey-io/valkey-ci-agent \
  --field repo=valkey --field version=9.1.0 --field stage=rc2 --field urgency=LOW

# GA after the final RC (the last rc tag must exist on the 9.1 branch)
gh workflow run release-notes-cut.yml --repo valkey-io/valkey-ci-agent \
  --field repo=valkey --field version=9.1.0 --field stage=ga --field urgency=LOW

# Patch GA (the 9.1.0 tag must exist on the 9.1 branch)
gh workflow run release-notes-cut.yml --repo valkey-io/valkey-ci-agent \
  --field repo=valkey --field version=9.1.1 --field urgency=LOW

# Module repo patch GA (the 1.2.1 tag must exist on the 1.2 branch)
gh workflow run release-notes-cut.yml --repo valkey-io/valkey-ci-agent \
  --field repo=valkey-search --field version=1.2.2 --field urgency=LOW
```

After reviewing the preview, repeat the same dispatch with
`--field dry_run=false` to open or update the release PR. The normal workflow
exposes only `repo`, `version`, `stage`, `urgency`, and `dry_run`; `stage` is
case-insensitive and is required only when the patch component is zero.

If more changes merge into `M.m` while that release PR is open, dispatch the
same `version` and `stage` again with `dry_run=false`. A rerun regenerates the
complete release range through the latest `M.m` tip, rebuilds the same
`agent/release-cut/<version>-<stage>` branch from that tip, and updates the
existing open PR in place. This deliberately re-evaluates earlier entries as
well as processing new ones, so changed labels or PR metadata are picked up.
Because the prep branch is replaced with `--force-with-lease`, manual edits made
directly on that generated branch are not retained.

Use `release-notes-cut-advanced.yml` only for an explicit date/baseline,
contributor override, security entries/advisory lookup, `no_align_prior_wording`,
or `force_ready`. It delegates to the same release workflow as the normal
dispatch, so the release logic cannot drift between the two interfaces.

**When one change ships on several release lines, the first line to *merge* its
release PR fixes the wording for the rest.** A backported change is worded once
and then reused verbatim by the other lines (see step 6 of **How it works**
below). The reuse reads merged release-line branches only, so a line cut while a
sibling's release PR is still open finds nothing to reuse and words the note
itself. Two consequences worth planning around:

- Cutting a line whose siblings have already merged is always the cheapest path
  to consistent notes; nothing else is required.
- If you cut several lines in parallel, re-dispatch the still-open ones after the
  first merges. A rerun regenerates from scratch, so it picks up the now-published
  sibling wording and converges the lines.

An omitted date resolves to the current **UTC** date. Use the advanced workflow's
explicit `date` input when the intended release date follows another timezone's
calendar day.

**Branch model** (tag-driven, one M.m branch per minor):

| Dispatch | Target branch | Range baseline |
|----------|---------------|----------------|
| rc1 of M.m.p | `M.m` | Previous release tag (auto-resolved) |
| rcN (N>1) | `M.m` | The `M.m.p-rc(N-1)` tag on M.m |
| ga of M.m.p | `M.m` | The last rc tag on M.m |
| later patches (`stage` omitted -> `ga`) | `M.m` | The previous patch tag (e.g. `M.m.(p-1)`) |

Maintainers create the M.m branch and push release tags before dispatching. The
agent never creates, deletes, or force-pushes a release-line branch; it creates
or updates only its namespaced prep branch.

### Common recipes

`version` and `urgency` are always required. `stage` is required for `M.m.0`
because the same version can mean rc1, a later RC, or GA; it is omitted for a
patch and inferred as `ga`. The target branch (`M.m`) and baseline resolve from
the version and repository tags.

| Cutting | `version` | `stage` | `base_ref` | Baseline the notes span |
|---------|-----------|---------|-----------|--------------------------|
| First RC of a new minor | `9.2.0` | `rc1` | *(empty)* | Auto: previous release tag (e.g. `9.1.0`) |
| Next RC on the same line | `9.2.0` | `rc2` | *(empty)* | Auto: the `9.2.0-rc1` tag on the `9.2` branch |
| GA after the final RC | `9.2.0` | `ga` | *(empty)* | Auto: the last rc tag (e.g. `9.2.0-rc2`) on `9.2` |
| Patch GA | `9.1.9` | *(empty -> `ga`)* | *(empty)* | Auto: the previous patch tag (`9.1.8`) |
| First RC of a new **major** | `10.0.0` | `rc1` | *(empty)* | Auto: the highest earlier release tag |

Distinctions that are easy to get wrong:

- **`version` is the target you are cutting, always `M.m.p`**: use `9.2.0` for
  every RC of that release (`rc1`, `rc2`, ...) and its GA, not `9.2.0-rc2`. The
  `-rcN` suffix comes from `stage`, not `version`. The M.m branch is derived from
  the version automatically; maintainers must create it and push tags before dispatch.
- **`base_ref` stays empty for normal cuts.** The tool resolves the baseline from
  release tags and shows the exact range in the PR body. Use the advanced
  workflow's override only to correct a range the preview proves is wrong.

Every normal dispatch defaults to `dry_run=true`: it logs the resolved plan and
the exact `base..head` range (both refs and their SHAs) without pushing or
opening a PR.

### How it works

The rendered commit lands on an agent-namespaced `agent/release-cut/...` prep
branch that opens a PR into the release line, so the line only advances when a
human merges.

1. **Resolve the plan** (code) - normalize an explicit stage, or infer `ga` when
   `PATCH > 0`; an omitted stage for `M.m.0` is rejected. Map that
   `(version, stage)` onto the branch model above. The version is canonicalized
   once (`M.m.p`, no leading zeros / stray whitespace) so the repository's
   version file, dated heading, commit title, and branch names all agree. The
   requested state must be newer than both the version file and every existing
   tag on that release line; an already-released stage or downgrade is rejected
   before the AI runs.
2. **Discover the range** (code) - resolve `base..head` and walk it by graph
   reachability, deduplicating to one entry per originating PR number. The M.m
   branch tip is fetched once and pinned to an immutable SHA used by discovery,
   contributors, and the prep worktree. The base is an explicit
   `--base-ref`, else tag resolution on the M.m branch: for rc1 it is the previous
   release tag (resolved from all tags in the repo), for rc2+ it is the prior RC
   tag (e.g. `9.2.0-rc1`), and for a patch GA it is the previous patch tag (e.g.
   `9.1.8`). Tags are created by maintainers before dispatch.
   Per-PR backports are credited to their merged source PR after provenance
   validation; supported evidence includes the structured backport summary,
   cherry-pick/subject/branch metadata, and a standalone
   `backport of <GitHub PR URL>` marker.
3. **Classify** (code) - split PRs by label: `release-notes` PRs are included
   directly, `no-release-notes` PRs are hard-excluded (dropped before triage, never
   noted, and listed in the PR body so a maintainer can catch a mislabel), and
   everything else is a triage candidate. If a PR carries both labels,
   `no-release-notes` wins.
4. **Triage** (AI) - Claude decides, per candidate without `release-notes`, whether the change
   is user-facing enough to note (include) or purely internal (exclude), with a
   short reason for each. Patch-release triage defaults uncertain correctness and
   safety fixes to inclusion. Code independently scans PR-authored title/body
   text for release-impact signals; if AI excludes such a candidate or omits its
   verdict, the guardrail forces it into generation as uncertain. Included
   candidates join the labelled PRs; all decisions and guardrail overrides are
   surfaced in the PR body. A candidate with no verdict and no guardrail signal
   falls back to human triage.
5. **Generate** (AI) - Claude writes one categorized, user-facing bullet per
   included PR (labelled + triaged-in). Category guidance favors the affected
   user-facing surface; code normalizes generic INFO/metrics/ACL LOG/logging
   classifications to `Observability and Logging` and flags that correction for
   review. The model never emits the `(#N)` reference or `by @handle`; code
   removes accidental duplicate markers and terminal punctuation, then appends
   the canonical attribution in `scripts/release_notes/render.py`.
6. **Align wording across release lines** (code) - a change backported to several
   release lines is discovered under its *original* source PR number on every
   line, so that trailing `(#N)` is a branch-invariant join key that code, not the
   model, produces. Before rendering, the cut reads the sibling release lines'
   already-merged `00-RELEASENOTES` out of the existing clone
   (`refs/remotes/origin/<M.m>`; no extra network calls, no writes) and, when a
   note's PR already has a published bullet there, replaces this cut's freshly
   generated prose with that published text verbatim. Only the prose is carried:
   the category, the `by @handle` credit, and the `(#N)` reference stay the ones
   this cut computed. Reuse is restricted to bullets under the canonical
   `### <category>` headings this tool writes, and to text that round-trips
   through the renderer unchanged, so hand-authored legacy sections are never
   mined. Every carry and every declined carry is listed in the PR body, the
   alignment tables last so an oversized body never clamps an actionable section. Two
   deterministic disagreements refuse the carry and hold the PR: the two lines
   crediting different authors, and the two wordings naming different environment
   boundaries (e.g. one says 32-bit, the other does not), which is the signature
   of a backport that was adapted rather than cherry-picked. `no_align_prior_wording`
   (advanced workflow) or `--no-align-prior-wording` opts a line out entirely.
7. **Render + bump** (code) - render the categorized bullets into a new dated
   section prepended before any existing sections on the release line via
   `render_release_notes` (`release_format.py`) / `set_version`
   (`version_bump.py`), append the cumulative contributor list
   (`contributors.py`) deduplicated by case-insensitive display-name/login
   identity (PR-resolved logins give squash-merged authors proper @handles),
   and bump the version file selected by the repository profile. These format
   primitives live
   in-repo rather than being imported from valkey, because upstream
   `valkey-io/valkey` ships no such tooling, so a cut runs against unmodified
   upstream (a plaintext `00-RELEASENOTES` placeholder and a `src/version.h`
   with the `VALKEY_VERSION*` macros).
8. **Open the PR** (code) - commit on the prep branch, push it (force-with-lease),
   and open/update a PR into the release line with a body that explains the cut and
   surfaces any advisories (below). When the cut flags anything a maintainer should
   address first, the PR opens as a draft to hold the merge (see [Edge-case
   handling](#edge-case-handling)). Immediately before changing the prep branch,
   the agent re-fetches M.m and aborts if its tip differs from the pinned SHA.
   Re-dispatching the same version/stage fully regenerates against the latest
   M.m tip, force-updates the deterministic prep branch, and edits the same open
   PR rather than creating another one.

### Edge-case handling

Malformed dispatch inputs fail fast at argparse (exit 2), before the clone + AI
run: a non-`M.m.p` or out-of-range version, an omitted stage for `M.m.0`, a bad
explicit stage, an urgency outside `LOW/MODERATE/HIGH/CRITICAL/SECURITY`, or a
non-ISO date. Repository-state validation runs after the clone: an explicit
`--base-ref` that resolves to nothing aborts with a clear error, and a cut
against a non-existent M.m branch is refused immediately. A target that is equal
to or older than the repository's version file, or at or behind an existing tag on that M.m
line, is also refused before note generation.

When the cut raises anything a maintainer should address before merging, the
release PR opens as a draft (GitHub refuses to merge a draft) so a shipped
change can never be released while a warning goes unread. The body leads with a
banner naming the held items, and each has its own section below. Resolve them and
click **Ready for review** to release, re-dispatch after resolving the signals,
or use `force_ready` from the advanced workflow to open ready anyway (the banner
then records that N items were overridden). `--dry-run` prints the same hold
decision without opening a PR. The signals that hold:

- **RC out of sequence** - a re-cut rc or a skipped rc number.
- **Unanchored baseline** - rc1 of `M.0.0` with no `--base-ref` fell back to the
  nearest tag, which may over-broaden the range.
- **Empty release notes** - no PRs in range, or no PR carried `release-notes` and AI triage
  included none of the remaining candidates; the body says which, so an empty dated
  section is not mistaken for a generation miss.
- **Duplicate / declined / low-confidence** - a PR credited in more than one
  bullet, any included PR for which the model produced no bullet, or a note the model flagged
  as low-confidence.
- **AI triage** - the model decided inclusion for PRs without `release-notes` (a call that used
  to require a human label), so the PR holds until a maintainer confirms the
  include/exclude table; a low-confidence triage call also holds.
- **Release-impact review** - a deterministic guardrail overrode an AI exclusion
  or missing verdict, or release-impact signals were detected while the requested
  urgency is `LOW`/`MODERATE`. The body lists every signal so release/security
  maintainers can choose urgency and any hand-authored Security Fixes entries;
  code does not assign severity automatically.
- **Security** - `SECURITY` urgency with no security fixes, or advisories that
  could not be read (a clean advisory match and normal-note deduplication for a
  supplied `--security-fix` are informational and do not hold).
- **Unresolved changes** - a shipped change that would otherwise slip past
  valkey's label-only gate: a range commit with no resolvable PR (absent from the
  notes entirely), a commit whose resolved PR could not be fetched, or a note
  credited to a backport PR because the original author's PR could not be recovered.
- **Triage** - PRs without `release-notes` that AI triage could not decide (no verdict returned), so
  a maintainer must decide whether to include them.
- **Release lines disagree about a note** - a sibling release line already
  published a bullet for this note's PR, but the two lines contradict each other
  about the change itself rather than only its wording: they credit different
  authors, or the two wordings name different environment boundaries. The carry is
  refused (this cut keeps its own wording) and the PR holds so a maintainer can say
  which line is right. Carries that succeeded, and carries declined for weaker
  reasons, are reported but do not hold.

The body always shows the resolved notes range so an over-broad baseline is
visible: the resolved mode (e.g. `rc2`), the source and target branches, and both
ends as `ref @ <sha>`, so a reviewer can audit the exact commits the notes were
computed over, not just the branch-model names. `--dry-run` prints the same range
and advisories to the log.

### Configuration

Reuses the same secrets and OIDC role as the other workflows (see
[DEVELOPMENT.md](DEVELOPMENT.md)). The workflow mints one
short-lived App token on `valkey-io/valkey` with `contents:write` (push the prep
branch), `pull-requests:write` (open the release PR),
and `metadata:read`. The advanced workflow exposes
`security_from_advisories`; when set, it mints a token that additionally holds
`repository-advisories:read`. That permission is requested only for an advisory
cut, so an ordinary cut is never blocked when the App installation lacks it.
The App installation must hold `repository-advisories:read` for an advisory cut
to read the advisories.

### Automated review comments

Leave an inline comment on the current `00-RELEASENOTES` change in an automated
release PR; no bot command is required. The hourly poll workflow scans
immediately, then every five minutes for 55 minutes. PR creation does not start
a timer, so a comment is normally found by the next scan plus any Actions queue
delay.

The poller batches every unresolved, current notes comment whose latest human
author is an active `valkey-io/contributors` member. It posts
`Addressing N release-note review comments.` on the PR and dispatches one
serialized handler run. A hidden head-SHA and batch marker prevents duplicate
work.

The handler re-fetches the PR and complete batch, runs one edit-only Claude pass,
and requires the edit to touch only the current dated notes section. It applies
the approved patch in a clean clone, rechecks the PR head and review batch, then
uses a normal fast-forward push. On success it updates the PR comment, replies
with the commit, and resolves only unchanged threads. Failed operations remain
visible on the PR and can be retried by a later poll. Addressing markers expire
after 90 minutes so a job that never reaches its handler cannot block the PR
forever. Batch identities include the complete review payload, so editing a
comment creates a new batch. Before pushing, the handler records the proposed
commit and review-thread identities; if the push succeeds but the job loses its
response or fails during bookkeeping, the poller finishes the replies and
resolutions idempotently without running another AI edit.

## Release automation

The release path is a short pipeline with one maintainer-facing tracking issue,
not a durable controller.

1. A maintainer dispatches **Prepare Release** from this repository's Actions
   tab. **Prepare Release** authorizes the dispatching maintainer against
   `valkey-io/valkey-committers` or `valkey-io/valkey-release`, derives the next
   version from the release line and
   existing tags, opens one `Release <tag>` tracking issue, and invokes the
   existing release-notes cutter. A real run opens or refreshes the
   deterministic `agent/release-cut/...` PR. Real preparation is the default;
   an explicitly selected dry run only displays the derived release.
2. The narrow **Refresh Release Progress** watcher derives status from live
   workflow, PR, branch, and release state. After the canonical release-notes
   PR is reviewed and merged, it dispatches **Publish Release** for that merge
   commit automatically. The issue only displays progress and the next action;
   its bot-owned status comment contains the controller metadata; the editable
   issue body and checkboxes cannot authorize a transition. Its hourly runner
   stays alive for 55 minutes and polls every five minutes, reusing the shared
   bounded polling mechanism while bridging delays in GitHub's scheduled-workflow
   queue and remaining below the App token's one-hour lifetime.
3. **Publish Release** requires the exact SHA to remain branch HEAD and to be
   the canonical preparation PR's merge commit. It verifies `version.h`, the
   release-notes section, advisory `ci.yml` status on that SHA, tag
   availability, and the latest-release decision. It then invokes the
   synchronous `valkey-release-automation` qualification workflow and waits
   for that exact call to pass before entering the protected `release`
   environment. After approval,
   it repeats every validation, checks that the approver is still a live
   `valkey-committers` or `valkey-release` member, requires the approved plan digest to match, atomically
   creates the tag at the candidate SHA, and publishes the GitHub release.

If the release branch moves after the PR merge, automatic publication stops and
the dashboard asks for a fresh preparation PR. If a build or qualification
fails, the dashboard links the failed run and gives the next action. The watcher
does not repair failures, infer success from issue text, or hold controller
state. Once production starts, the same dashboard also exposes deterministic
links for every applicable downstream target: hashes, container, documentation,
website, Helm, and Bundle. The production run remains the completion authority;
the links add follow-up visibility without granting the controller App access to
those repositories.

The GitHub release event starts the normal production workflow in
`valkey-release-automation`. That workflow resolves the published tag to its
commit and requires any supplied SHA to match. It retains the complete production fan-out: archives, RPM/DEB
packages, hashes, container, docs, website, Try Valkey, Bundle, and a draft Helm
chart bump for GA releases. The Helm PR remains draft until the matching public
container image is available; that small human ordering check replaces the
controller's long-lived post-release polling loop.

### Security setup

- Configure GitHub Actions actor policy so only release maintainers can run
  the manual workflows. The live `valkey-committers` / `valkey-release` check
  remains defense in depth.
- `release-control` must allow only the exact default branch. The release
  workflows authenticate with the `VALKEYRIE_BOT_APP_ID` and
  `VALKEYRIE_BOT_PRIVATE_KEY` secrets (shared with the other agent
  workflows), plus the release-notes Bedrock credentials. Qualification
  is a secretless synchronous reusable workflow call, so it needs no
  cross-repository Actions-write token.
- Grant the App only the permissions requested by the workflows:
  - on `valkey`: `administration:read`, `actions:read`, `checks:read`,
    `contents:write`, `issues:write`, `metadata:read`, and
    `pull-requests:write`;
  - on each module repository (`valkey-search`, `valkey-json`, and
    `valkey-bloom`): `contents:write`, `metadata:read`, and
    `pull-requests:write`;
  - on any repository using advisory-backed cuts:
    `repository-advisories:read`;
  - on `valkey-ci-agent`: `contents:write`, `issues:write`, `metadata:read`,
    and `pull-requests:write`;
  - on `valkey-release-automation`: `actions:read`, `contents:read`, and
    `metadata:read`;
  - at organization scope: `members:read` for release authorization and
    `organization-projects:read` for first-GA backport onboarding.
- `release` must allow only the exact default branch, require a reviewer,
  prevent self-review, and disallow admin bypass. The publication job mints
  its token there, and the App named as the tag-ruleset bypass needs
  `administration:read`, `actions:read`, `checks:read`, `contents:write`,
  `metadata:read`, and `pull-requests:read`, plus organization
  `members:read`, on `valkey`.
- The workflow verifies the tag ruleset is ACTIVE and IMMUTABLE (restricts
  creation, update, and deletion for release tags) before publishing, which
  needs only `administration:read`. The ruleset's bypass list is deliberately
  not enumerated: GitHub reveals `bypass_actors` only to tokens that could
  edit the ruleset, so enumerating it forced `administration:write` onto an
  unattended credential, and the drift it would detect is caused by the same
  repository-admin role that can edit or delete the ruleset outright. Keep
  the bypass list to exactly the publication App by admin review.
- Current deployment runs control and publication as ONE App (the Valkeyrie
  Bot). To split the roles without code changes, create a dedicated
  publication App, store its id/key as environment-scoped
  `VALKEYRIE_BOT_APP_ID` / `VALKEYRIE_BOT_PRIVATE_KEY` secrets inside
  `release` (environment secrets shadow repository secrets of the same
  name), and re-point the ruleset bypass to it.
- `valkey-release-automation` keeps its existing `release-publish`
  production approval. It is the second deployment approval before public
  packages and downstream updates are written.
- GitHub environments require only one of their configured reviewers. The two
  approvals here are the separate `release` and `release-publish` gates,
  not two names listed on one environment.

For a maintainer, the normal path is: dispatch **Prepare Release** here,
merge the notes PR, approve `release`, and approve `release-publish`. Everything
between those decisions advances automatically, and the tracking issue remains
the single place to find the current run, failure, and next action.

### Aborting a release

Close the release-tracking issue to abandon that release. The hourly progress
workflow scans only open trackers, so closing the issue stops reconciliation and
prevents any new publication dispatch. If an operator needs to stop all release
reconciliation while investigating an incident, disable **Refresh Release
Progress** in `valkey-ci-agent`; re-enable it after every abandoned tracker has
been closed. There is no `/release abort` command.

Deploy `valkey-release-automation` before this CI-agent change: its public
`qualify-release.yml` must expose `workflow_call` when **Publish Release** invokes
it. The CI-agent repository's Actions policy must allow first-party reusable
workflows from `valkey-io`.

## CVE Scan Workflow

Verification instead of prediction. The scan finds candidate CVEs in the published container images; we then build the candidate image ourselves, scan the real artifact to prove the targeted CVEs are gone, and only then dispatch valkey-container's plain build-and-publish workflow. We never predict whether a rebuild would help by inspecting base package databases or simulating installs; we build and check. All findings are reported in the GitHub Actions job summary. No GitHub issues are created.

This is Phase 1: a concrete implementation targeting `valkey-io/valkey-container`. Phase 2 (reusable `workflow_call` extraction for other repos) is planned.

### How it works

The workflow has four jobs:

1. `scan` scans every published architecture, reports findings, and emits one JSON `plan`. Each plan leg contains `{line, variant, platform, cves}` and is used directly as the verification matrix.
2. `verify` calls valkey-container's shared build action with `push: false` and `load: true`, then checks the rebuilt artifact for that leg's CVE IDs. It writes a uniquely named `verified`, `survivors`, or `error` marker.
3. `collect` reconciles those markers against the same plan. Missing and unexpected legs stay visible; a line qualifies when any affected architecture is verified.
4. `rebuild` dispatches valkey-container's normal `ci.yml`, locates the correlated run, waits for its real result, and reports the per-architecture status.

The verify job has no credentials and cannot publish. Only the rebuild dispatch step receives the scoped GitHub App token. See [CVE Scan Flow](docs/architecture.md#cve-scan-flow) for the complete contract, failure behavior, and security boundaries.

### Tradeoffs

The image is built once for verification and again for publication. This keeps publication decisions out of valkey-container, at the cost of extra builds. Verification covers every affected architecture, while dispatch remains any-architecture because `ci.yml` rebuilds all platforms together. The report therefore never treats a partially verified line as fully fixed.

### Installation

#### Prerequisites

- The **Valkeyrie Bot GitHub App** installed on the target repository with:
  - `actions: write` (dispatch the rebuild workflow)
  - `contents: read`, `metadata: read`
- Org-level secrets: `VALKEYRIE_BOT_APP_ID` and `VALKEYRIE_BOT_PRIVATE_KEY`

#### Step 1: Configure secrets

On the repo hosting the agent workflows:

| Type | Name | Value |
|------|------|-------|
| Secret | `VALKEYRIE_BOT_APP_ID` | Valkeyrie Bot GitHub App ID |
| Secret | `VALKEYRIE_BOT_PRIVATE_KEY` | App private key |

Forks are scan/dry-run only: there is no PAT fallback, so without the org App secrets the rebuild job's guards keep it from running and no token is minted.

#### Step 2: Create the protected Environment

Create a `cve-rebuild-dispatch` Environment in repo Settings with a deployment-branch rule limited to `main`. It is a credential boundary (not an approval gate): it scopes the App credentials and dispatch permission so no other ref can access them.

### Configuration

Settings are loaded from `CVE_SCAN_*` environment variables with sensible defaults
targeting `valkey-io/valkey-container`. The workflow pins all values explicitly in
its `env:` block (house style: visible-in-workflow configuration). Override any
variable to change behavior for forks or testing.

| Variable | Default | Description |
|----------|---------|-------------|
| `CVE_SCAN_VERSIONS_URL` | `https://raw.githubusercontent.com/valkey-io/valkey-container/mainline/versions.json` | URL to the versions.json manifest for dynamic image resolution |
| `CVE_SCAN_REPOSITORY` | `valkey/valkey` | Docker Hub repository prefix for derived image tags |
| `CVE_SCAN_INCLUDE_UNSTABLE` | `false` | Include the `unstable` version line (truthy: `1`, `true`, `yes`, `on`; falsy: `0`, `false`, `no`, `off`, empty) |
| `CVE_SCAN_SEVERITY_THRESHOLD` | `HIGH` | Ignore findings below this severity (`UNKNOWN`, `LOW`, `MEDIUM`, `HIGH`, `CRITICAL`). |
| `CVE_SCAN_IMAGES` | *(empty)* | Optional static image list (comma-separated). When set, overrides dynamic resolution from versions.json. Testing/escape hatch. |
| `CVE_SCAN_PLATFORMS` | `linux/amd64,linux/arm64,linux/arm/v7,linux/ppc64le` | Comma-separated platforms to scan per image. Defaults to the verified published set for valkey images. |

Invalid values (bad severity, malformed booleans, or empty platform lists) raise immediately:
a typo must not silently scan nothing.

### Usage

#### Weekly scan (automatic)

Runs weekly on the configured schedule (default: Monday 06:00 UTC). Scans all images in the matrix on all configured platforms, reports findings in the job summary, builds and scans each candidate to prove the fix, and dispatches a targeted rebuild automatically for the candidates that pass verification.

#### Manual scan

```bash
gh workflow run cve-scan.yml --repo <agent-repo>
```

Supports a `dry_run` input that prints findings without dispatching a rebuild. It defaults to `true`: a manual run never triggers a real rebuild unless you explicitly set `dry_run=false`. Supports a `severity_threshold` input for ad-hoc investigation; it affects classification and therefore rebuild dispatch, not just reporting.

#### Reviewing results

After each scan, check the workflow run's job summary in GitHub Actions. The summary lists all findings (candidates and not-fixable) as grouped markdown tables. Candidates flow into the `verify` job, which builds and scans the real artifact to prove the fix; only proven candidates reach the `rebuild` job, which then waits for the downstream valkey-container build and reports its actual conclusion and run URL in the job summary and Slack, so any non-success outcome (failed, cancelled, or timed-out) always notifies and is visible rather than masked by a successful dispatch.

## Safety

- **Branch namespace** - the agent writes only `backport/...` (direct
  backports), `agent/backport/...` (sweep backports), `agent/ci-fix/...` (CI
  fixes), and `agent/release-cut/...` (release cuts) branches and opens PRs for
  maintainer review. It never force-pushes a release line directly.
- **Credential isolation** - all GitHub auth uses `GIT_ASKPASS`; tokens never appear in `.git/config` or URLs
- **Claude Code env isolation** - `GITHUB_TOKEN`, `GH_TOKEN`, and `*_SECRET` are stripped from the subprocess environment. Claude cannot see credentials.
- **Deterministic validation** - registry-configured build commands run before push. A validation failure blocks the push.
- **CVE scan: proof, not prediction** - a rebuild is dispatched only after the `verify` job builds the candidate image itself and scans the real artifact, and only for the version lines `collect` finds proven fixed on at least one affected architecture. A run that proves nothing at all (no architecture verified anywhere, with an errored or missing leg) fails closed and dispatches nothing. The workflow run log is the audit record for every automatic dispatch.
- **CVE scan: targeted dispatch** - rebuilds are dispatched with `--field version="<versions>"` for only the affected version lines (e.g. `8.0 9.1`), not a rebuild-all. This minimizes the blast radius of automatic rebuilds.
- **CVE scan: no registry credentials in verify** - the `verify` job builds with `push: false` / `load: true` and holds no registry credentials, so the candidate image never leaves the runner.
- **CVE scan: no AI in the pipeline** - the entire scan-verify-dispatch path is deterministic code (scanner, shared candidate build action, `verify_candidate`, `gh workflow run`). No AI layer participates in any decision or dispatch step.
- **Fork sync** - when a different-owner `push_repo` is configured, the agent fast-forwards that fork's release branch to match upstream before cherry-picking
- **Stale branch pruning** - if a previous backport PR was closed without merging, the agent deletes the orphaned branch before starting fresh
- **DCO** - backport commits are signed off. ci_fix commits are authored by the bot without a sign-off, so a human certifies the change before merge.

## Documentation

- [docs/architecture.md](docs/architecture.md) - full system design including planned workflows
- [DEVELOPMENT.md](DEVELOPMENT.md) - local setup, testing, and GitHub Actions usage
