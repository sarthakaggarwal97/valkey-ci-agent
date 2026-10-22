# CI fix

Fixes one failed CI job per run. The bot binds the request to one exact
failed job, reproduces the failure, lets one Claude Code session diagnose and
edit, verifies and reviews the candidate in a loop, and then publishes it the
way backports are published: a commit on a bot-owned branch, with the target
PR's normal CI and a human review deciding whether it merges. The bot never
merges.

Where it can, the fix is prepared inside the very CI job that failed, on that
job's runner, after that job's own setup and build, with that job's own test
command.

## Front doors

Three front doors build the same request; a fourth is a manual dispatch.

| Front door | Workflow | Runs | Publishes to |
|---|---|---|---|
| Maintainer command on a PR | `ci-fix-comment-poll.yml` -> `ci-fix.yml` | poll hourly, or by hand | The PR: a commit on a bot-owned branch, or a patch comment on a contributor or fork PR |
| Bot-owned PR whose CI went red | `backport-ci-followup.yml` | hourly | The PR, with a commit |
| Test Failure Detector issue | `ci-fix-issue-dispatch.yml` -> `ci-fix-issues.yml` | dispatcher hourly (inert until enabled), or by hand | The issue, as a patch comment, or a draft PR |
| Manual request for a PR | `ci-fix.yml` | by hand | As for a maintainer command |

**Maintainer command.** An active member of `valkey-io/contributors` puts this
at the start of a comment on a `valkey-io/valkey` PR:

```text
@valkeyrie-ops fix [<run-or-job-url>] [hint]
```

(`@valkeyrie-bot fix` is accepted too.) The poller only authorizes, claims the
comment with a reaction, and dispatches `ci-fix.yml`. A run or job link selects
that exact failure. Without one, the gate picks the most actionable completed
failure on the PR's current head: a plain test job first, then a lint or build
job (whose fix is often in CI definitions the bot never changes), then a
sanitizer or valgrind job.

**Bot-owned PR CI.** For each repository with `automatic_ci_followup`, the
follow-up selects one unhandled failed Actions job on the current head of an
open bot-owned PR: the sweep PR, a CI fix it pushed, or a draft fix PR it
opened. Selection rules are in [backport.md](backport.md#automatic-ci-follow-up).

**Detector issue.** See [Detector issue fixes](#detector-issue-fixes).

Only a same-repository, bot-authored PR under `backport/...`,
`agent/backport/...` or `agent/ci-fix/...` is writable. Every contributor or
fork PR is a suggestion-only target: it receives a concrete patch in a comment
and its branch is never changed.

## The pipeline

Every front door runs the fix as three jobs of `ci-fix-run.yml`
(`scripts/ci_fix/run.py`):

```text
plan -> fix-in-job | fix-in-container | fix-local -> publish
```

- **plan** binds the request to one exact failed job, resolves that job from
  its workflow file at the commit the fix is prepared on (see
  [The failed job](#the-failed-job)), and hands it, with the job's log, to the
  next job as an artifact.
- **fix-in-job** is the failed job itself, on its runner (`fix-in-container`
  when it ran in a container). The job's own setup and build run, then the
  fix is prepared in that checkout with the job's own build and test commands.
  A failure its job cannot host (a VM job, a step or expression the resolver
  does not follow, or a suggestion, which is never run) goes through the same
  steps on the bot's own Ubuntu runner (`fix-local`). None of these jobs holds
  a GitHub token.
- **publish** pushes, comments or opens the draft PR, and reports the result
  where the request came from, also when the fix job failed or never finished.

## How it works

1. **Bind one task.** The front door records the repository, run attempt,
   exact failed job ID and name, tested SHA, and publication target. A PR
   request requires the run to match the PR's current head.
2. **Load exact evidence.** Code takes one immutable job snapshot for the
   selected run attempt, downloads that job's log (not an aggregate run log),
   and checks out the commit the candidate will be prepared on: the failed
   job's own checkout, or a clone.
3. **Look for an existing fix.** Before anything is edited, code searches the
   checkout's history (every branch is there) for merged commits that name the
   failing test, change its file or a source file the log names (as a compile
   error does), or reference its issue. On a release branch a match on the
   default branch is a port candidate unless it was already cherry-picked; on
   the default branch a match after the failing run means the failure may
   already be fixed. Candidates and their diffs go to the agent as evidence,
   not as a decision.
4. **Reproduce.** For a test in Valkey's Tcl suite or a C unit test, code
   builds the way the failed job did and runs only the failing tests on the
   unfixed code:

   ```text
   ./runtest --single <unit> --only '<test>' --loops N --fastfail
   ./src/unit/valkey-unit-gtests --gtest_filter='Suite.Test' --gtest_repeat=N
   ```

   Once (twice if it fails, to tell a frequent flake from a deterministic
   failure), then 20 loops (2 under valgrind), then the same loops with every
   core busy. A file that uses `run_solo` loops with one test client. A run
   counts only if the output shows that test failing, or a valgrind or
   sanitizer report; a build break or harness exception does not. Branches
   whose `runtest` lacks `--fastfail` (7.2) run every loop to the end.
5. **Diagnose and edit.** One Claude Code session, told what steps 3 and 4
   found and given Valkey's test conventions
   ([`scripts/ci_fix/guides/valkey-io__valkey.md`](../scripts/ci_fix/guides/valkey-io__valkey.md)
   plus the checkout's own `tests/README.md` and contributor docs), chooses
   one structured action: port a merged fix (preferred when one fits), make a
   minimal patch, or refuse. It may edit its disposable checkout but cannot
   commit or push.
6. **Package the candidate.** Code rejects agent-created commits, undeclared
   or mismatched changed paths, incomplete diagnoses, malformed port SHAs, and
   oversized suggestions. A port is independently checked to be merged on the
   default branch and absent from the target head.
7. **Verify and review** (bot PRs and detector issues only; see below).
8. **Publish.** For a bot PR, code reapplies the patch or cherry-picks the port
   in a clean clone and pushes with an exact-head `--force-with-lease`. For a
   contributor or fork PR, it posts the patch without touching the branch. For
   a detector issue, it posts the patch on the issue or opens a draft PR. The
   comment lists what the bot checked, including the existing-fix search and
   the reproduction.

### The verification loop

Before a patch or port for a bot PR or a detector issue is published, it goes
through up to three rounds within a 60-minute budget (reproduction may use 20):

1. A port must be merged on the default branch and is cherry-picked into the
   checkout. One that conflicts goes back to the agent, which may adapt it as
   a patch.
2. With the candidate applied, the reproduction runs again, looped and under
   load if that is how it reproduced: 10 times for a failure seen on both
   plain runs, 40 for an intermittent one or one the agent judged flaky (4
   either way under valgrind). When code's reproduction never failed, or there
   was none, the agent's own test command, if it gives one, must fail without
   the patch (tried up to twice, or ten times for a flaky failure) and pass
   with it as many times.
3. A separate read-only review session, given the same conventions and the
   reproduction result, must approve. Each round leaves 10 minutes for this
   step and the previous one.
4. A failed check or a rejection goes back to the agent with the test output
   or the review's reasoning, and the round repeats.

A failure that did not reproduce can still be fixed from the log, but the
report says the fix is unproven. A candidate that does not pass within the
rounds is refused. A mistake in what the agent hands back (an undeclared path,
a short SHA) goes back to it like a failing test. Running out of time, or a
crash of the agent or its review, is a failed attempt rather than a refusal:
the follow-up and a detector issue each get a second attempt for the same job
or occurrence. A suggestion for a contributor or fork PR is reviewed but never
run.

### Isolation

- The fix job holds no GitHub token; the `plan` and `publish` jobs each mint a
  short-lived, repository-scoped App token for their reads and writes. Claude
  Code never receives GitHub credentials; code owns commits and pushes.
- Every CI-fix agent (edit, review, suggestion) and every build and test
  command runs as a separate `ci-fix-agent` account
  (`scripts/common/isolation.py`) that cannot read the workflow user's
  environment. The agent is started with only the model's AWS session
  credentials, and build and test commands with none.
- That account owns the checkout while it runs. Code refuses to continue if
  `.git` was replaced (a planted config's filter or merge driver would
  otherwise run as the workflow user), and before verifying removes every
  ignored file the job's own setup did not leave as it is, so a planted
  binary cannot stand in for the build.
- Only the exact change that was checked is accepted. If the build or the
  agent's test command changed tracked files during verification, the tree
  is a different candidate and goes back to the agent.
- Valkey's build and test dependencies (Tcl, `tcl-tls`, valgrind, OpenSSL)
  are installed before any credential exists.
- A change under `.github/` is never pushed, and the tokens carry no
  `workflows` scope.

## The failed job

To fix a failure in the environment that failed, `scripts/ci_fix/ci_job.py`
reads the failed run's workflow file (`daily.yml` for a scheduled failure, the
PR run's own workflow otherwise) at the commit the fix is prepared on (a PR's
head, or the branch tip for a detector issue) and finds the job instance whose
name is the failed job's; a matrix entry is told by the name GitHub gives it.
The `${{ }}` expressions the job needs are evaluated as GitHub would for that
run, with empty dispatch inputs.

The agent can run in a job on an Ubuntu or macOS runner, or in a container on
an Ubuntu runner, but not in a VM, with one `make` step and one `runtest`
command, and setup steps that are scripts or checkouts. The fix job checks out
the commit the fix is prepared on and the job's one other repository
(`libbacktrace`), then replays the job's setup steps up to its tests, as
GitHub runs them, from the plan's `setup.sh`. Instead of the tests, the agent
then prepares the fix in that checkout (`scripts/ci_fix/job.sh`), rebuilding
with the job's `make` command and testing with its `runtest` line.

A step, matrix or expression the resolver does not follow exactly (a secret,
another step's output, an action other than a checkout, a script between the
tests) means the job cannot host the fix, which is prepared on the bot's
runner instead. The fix job receives only `contents: read`, the OIDC token and
`AWS_ROLE_ARN` for the model.

## Detector issue fixes

`ci-fix-issues.yml` takes one `issue=<N>` per dispatch. The front door
requires an open, unassigned, detector-authored issue with no open linked PR,
parses its latest run marker, verifies that it names a scheduled `daily.yml`
run, checks that the bot has not already reported on that occurrence (so a
dispatch retried after a lost response cannot post a second patch), and
resolves the recorded failure to one exact failed job. The candidate
is prepared on the current tip of the branch that run tested (`unstable`),
with the tested SHA and exact job log kept as evidence. Only that issue's test
is reproduced and verified; other tests the same job failed have issues of
their own.

`ci-fix-issue-dispatch.yml` picks the issues. Each tick it reads the newest
open detector issues in `valkey-io/valkey` and dispatches a run for each one
that has none queued or running, is unassigned, has no open linked PR, and
has, for the occurrence it records, no bot fix or refusal and fewer than two
failed attempts (a newer occurrence makes it eligible again). At most
`CI_FIX_ISSUES_MAX_RUNS` (default 2) issue runs are active at once. Its hourly
schedule does nothing until `CI_FIX_ISSUES_AUTO_DISPATCH` is `true`; manual
dispatches default to a dry run that only lists what it would start.

How a found candidate is published is `CI_FIX_ISSUES_PUBLISH`'s choice:

- `comment` (default): the verified patch is posted on the issue with the root
  cause, the fix and what the bot checked. Nothing is pushed and no PR is
  opened; a maintainer applies the patch, or not. The occurrence counts as
  handled, so the issue is not tried again until the test fails anew.
- `draft-pr`: code creates a unique `agent/ci-fix/issue-<issue>-<run>` branch
  and opens a draft PR against the tested branch containing `Fixes #<issue>`.
  One test file often holds several open failures, so when the patch changes
  the test file of another open detector issue, that issue's own test is
  reproduced and verified separately: it must fail without the patch and pass
  with it before the PR says `Fixes` it. An issue whose test the patch changed
  without that being shown is named in the body with no `Fixes` line.
  `publish` rechecks the issue right before opening the PR.

The PR front doors are unaffected: a maintainer's command and a bot-owned PR's
follow-up still update the PR, or suggest a patch on it.

## Running it

**Fix a failure on a PR by hand** (the dispatching user is authorized like a
commenter, so must be an active member of `valkey-io/contributors`; `repo`
must be `valkey-io/valkey`; leave `run_url` empty to pick the most actionable
failure on the current head):

```bash
gh workflow run ci-fix.yml \
  --repo valkey-io/valkey-ci-agent \
  --field repo=valkey-io/valkey \
  --field pr=1234 \
  --field run_url=https://github.com/valkey-io/valkey/actions/runs/123456789/job/987654321
```

**Scan for commands now** instead of waiting for the hourly poll:

```bash
gh workflow run ci-fix-comment-poll.yml \
  --repo valkey-io/valkey-ci-agent \
  --field lookback_minutes=180
```

**Fix one detector issue:**

```bash
gh workflow run ci-fix-issues.yml \
  --repo valkey-io/valkey-ci-agent \
  --field issue=1234
```

**See which issues the dispatcher would start**, without starting any:

```bash
gh workflow run ci-fix-issue-dispatch.yml \
  --repo valkey-io/valkey-ci-agent \
  --field dry_run=true
```

The backport follow-up front door is dispatched as in
[backport.md](backport.md#running-it).

## Configuration

The fix workflows reuse the shared secrets and OIDC role
([setup.md](setup.md)). `ci-fix.yml` and `ci-fix-issues.yml` run only when
dispatched; `ci-fix-comment-poll.yml` and `backport-ci-followup.yml` are
hourly; `ci-fix-issue-dispatch.yml` is hourly but inert until enabled.

| Variable | Default | Purpose |
|---|---|---|
| `CI_FIX_AGENT_USER` | `ci-fix-agent`, set by `scripts/ci_fix/job.sh` | Account the agent and its test commands run as; required inside Actions |
| `CI_FIX_ISSUES_AUTO_DISPATCH` (repository variable) | unset | `true` lets the issue dispatcher's schedule start fixes |
| `CI_FIX_ISSUES_MAX_RUNS` (repository variable) | `2` | Issue fixes active at once |
| `CI_FIX_ISSUES_PUBLISH` (repository variable) | `comment` | `comment` posts the patch on the issue; `draft-pr` pushes a branch and opens a draft PR |
| `CI_AGENT_CLAUDE_MODEL` (repository variable) | unset | Model override for the agents, for example `opus` |

Per-repository: `automatic_ci_followup` and `ci_followup_ignored_jobs` in
[`repos.yml`](registry.md). `ci-fix.yml` and its poller are scoped to
`valkey-io/valkey`; the engine is mostly repository-agnostic (job resolution
and reproduction follow Valkey's `make` and `runtest` conventions), but
onboarding another repository needs its own token and trigger configuration.
Detector issue handling has no registry setting.

## Internals

Three front doors construct the same typed `FixTask`; `policy.py` holds one
exhaustive `TargetKind` capability map (agent profile, patch limit, port
support, push permission, branch creation) and `publish.py` branches on it to
one of three small handlers. Adding a target means a capability record and a
handler. The front doors share state-reading primitives:
`github_runs.jobs_for_run_attempt()` returns one immutable job snapshot for an
exact rerun attempt; `github_runs.newest_runs_for_head()` owns branch/SHA
filtering and superseded-run collapse; `issue_state.py` owns detector
occurrence parsing and linked-PR inspection. The plan, the resolved job and
the fix's outcome pass between jobs as JSON (`state.py`).

```text
front-door gate
  -> FixTask
       Failure:   repo, run id + attempt, exact failed job id + name, tested SHA
       FixTarget: update bot PR | suggest on contributor PR | fix a detector issue
       FixPolicy: backport | general

plan
  -> workflow_artifacts.download_job_log(exact job id)
  -> ci_job.resolve(the failed job, from its workflow file) -> setup.sh

engine.prepare_fix
  -> the job's own checkout, set up and built by setup.sh; or, for fix-local,
     shallow_clone_at_sha(expected publication commit) with full blobless history
  -> failing tests = front door's (detector issue) + the log's [err] lines
  -> prior_fixes.find_prior_fixes(checkout, tests, tested SHA, issue)
  -> reproduce.plan_for(job name, tests) + reproduce(unfixed checkout)
       once, looped, looped under load; reproduced | not | unrunnable
  -> loop, at most MAX_ROUNDS (3):
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
  -> suggestion:      revalidate the open PR and post the concrete patch
  -> existing bot PR: revalidate ownership, branch and exact head; apply the
                      patch or cherry-pick the port in a clean clone; push one
                      descendant with an exact force-with-lease
  -> detector issue:  revalidate the occurrence and source-branch head; post
                      the patch, or create agent/ci-fix/... and open a draft PR
  -> render the terminal result; never merge
```

Budgets: `PREPARE_BUDGET_S` (60 minutes), `REPRO_BUDGET_S` (20 minutes),
`MAX_ROUNDS` (3), `TEST_RUNS` (2), `FLAKY_TEST_RUNS` (10). Agent profiles:
`ci_fix_edit` (has Bash), `ci_fix_review` (read-only), `ci_fix_suggest`
(no Bash or Glob); see [architecture.md](architecture.md#ai-layer).

Entry points:

- `scripts/ci_fix/run.py` - one fix as three jobs (plan, fix, publish) for every front door
- `scripts/ci_fix/main.py` - authorize and plan one PR request, and report its result
- `scripts/ci_fix/comment_poll.py` - find fix commands on PRs and dispatch `ci-fix.yml`
- `scripts/ci_fix/issues.py` - validate and plan one detector issue, and report on it
- `scripts/ci_fix/issue_dispatch.py` - start one issue fix per eligible open Daily issue, up to a cap
- `scripts/ci_fix/ci_job.py` - resolve the failed job from its workflow file (matrix entry, expressions, setup steps, build and test commands) and write the script that replays its setup
- `scripts/ci_fix/job.sh` - run the fix inside the job that hosts it: the agent's account and checkout
- `scripts/ci_fix/state.py` - JSON state carried between a fix's jobs
- `scripts/ci_fix/engine.py` - exact-log checkout, existing-fix search, reproduction, fix/verify/review loop
- `scripts/ci_fix/prior_fixes.py` - find merged commits that may already fix the failing tests
- `scripts/ci_fix/reproduce.py` - build and loop the failing Tcl tests, or repeat the failing C unit tests, with their job's commands or the closest
- `scripts/ci_fix/guides/` - per-repository test conventions for the fix agent and its reviewer
- `scripts/ci_fix/agent.py` - diagnosis-and-edit prompt, review prompt, and structured result parser
- `scripts/ci_fix/publish.py` - target revalidation and publication
- `scripts/ci_fix/push.py` - clean-clone patch/port commit and namespace-restricted push
- `scripts/ci_fix/policy.py` - target capabilities, bot identities, and writable namespaces
- `scripts/ci_fix/gate.py` - command parsing, fail-closed team auth, SHA-bound run gating
- `scripts/ci_fix/github_runs.py` - exact-attempt job snapshots and current-head run selection
- `scripts/ci_fix/issue_state.py` - detector occurrence and linked-PR state readers
- `scripts/ci_fix/comment.py` - render a pushed fix, suggestion, refusal, or failure
- `scripts/ci_fix/models.py` - `Failure`, `FixTarget`, `FixTask`, `FixCandidate`, and `FixResult`
