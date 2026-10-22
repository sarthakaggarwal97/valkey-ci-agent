# Backport

Cherry-picks merged PRs onto release branches, resolves conflicts with Claude
Code, validates the result, and opens one rolling PR per release branch for a
maintainer to merge. Works for any repository in [`repos.yml`](registry.md):
Valkey core, the modules (bloom, search, json), or anything else that tracks
backports on a per-branch GitHub Project board.

## Workflows

| Workflow | File | Runs | Purpose |
|---|---|---|---|
| Backport Sweep | `backport-sweep.yml` | daily 09:00 UTC, or by hand | Top up every branch's sweep PR with validated cherry-picks |
| Backport Poll | `backport-poll.yml` | hourly | Re-run the sweep for a branch as soon as its sweep PR has merged |
| Backport Mark Done Poll | `backport-mark-done-poll.yml` | hourly (:30) | Move project-board items to "Done" once the backport landed |
| Backport CI Follow-up | `backport-ci-followup.yml` | hourly (:20) | Hand one failed CI job on a bot-owned PR to the [CI fix](ci-fix.md) engine |
| Backport Eligibility | `backport-eligibility.yml` | daily 00:30 UTC | Comment on newly merged PRs that look worth backporting |
| Manual Backport | `manual-backport.yml` | by hand | Backport one PR to one branch |
| Manual Revert Commit | `manual-revert-commit.yml` | by hand | Revert one commit on a sweep branch |
| Backport PR | `backport.yml` | `workflow_call` | The reusable single-PR job behind Manual Backport |

The bot writes only `backport/...` and `agent/backport/...` branches, never a
release branch, and never force-pushes a published sweep branch.

## The sweep

1. **Fan out.** The preflight job reads `repos.yml` and starts one matrix leg
   per `{repo, branch}`.
2. **Discover.** Each leg queries the branch's GitHub Project v2 board for PRs
   with status "To be backported".
3. **Cherry-pick.** Each candidate is cherry-picked onto the sweep branch
   `agent/backport/sweep/<branch>`. Merge and squash merges are resolved to
   their source commits; rebase merges are rejected.
4. **Resolve conflicts.** When a cherry-pick conflicts, Claude Code reads both
   sides and resolves the conflict in place (profile
   `conflict_resolve_edit_only`). A candidate touching more than
   `max_conflicting_files` is dropped.
5. **Validate.** The registry's build and validation commands run against the
   candidate's own diff. A failure removes that candidate from the branch.
6. **Open or update the PR.** The branch is pushed and a PR with a summary table
   is opened or updated. Candidates that were skipped or failed validation are
   listed in the PR's "Needs attention" section, not committed.
7. **Follow up on CI.** For repositories with `automatic_ci_followup`, a
   failed Actions job on the PR's current head goes to the CI fix engine
   (see [Automatic CI follow-up](#automatic-ci-follow-up)).
8. **Mark done.** Once the backport has landed on the release branch, the
   source PR's board status moves from "To be backported" to "Done".

Each scheduled run keeps up to two validated cherry-picks
(`--max-candidates 2`). Backport commits carry a DCO sign-off; conflict
resolutions carry the `llm_conflict_label` label on the PR.

### Keeping the sweep branch green

A candidate is kept only if the whole branch still validates after it is
applied. A failing candidate is reset off the branch, so one bad commit never
blocks the ones after it.

Validation, in order:

- `validation_setup_commands` run once per clone.
- `build_commands` run before any push; a non-zero exit blocks the push.
  Repositories with none rely on their upstream CI.
- `validation_rules` map changed paths to commands.
- A `validation_profile` derives checks from the exact changed files. The
  `valkey-core` profile runs `git diff --check` over the candidate range,
  branch-native `clang-format-18` over only the candidate's line ranges and
  the suffixes that release's workflow enforces, the directly changed Tcl
  tests in their required modes (TLS included), C/C++ unit tests,
  branch-compatible checks for cluster, sentinel, module or test-harness
  changes, and reply-schema validation for changed tests that do not opt out.
  It follows the target branch's own C/C++ unit-test discovery convention, so
  a clean cross-harness cherry-pick cannot pass while its new test file is
  ignored.
- `generated_file_rules` run their generator twice to prove convergence. A
  generator may edit only its declared tracked outputs; an allowed update is
  amended into the candidate commit, anything else fails closed.

Two AI-assisted recoveries exist, both opt-in through
`repair_validation_failures`:

- **Repair.** Claude Code gets one edit-only attempt, scoped to the backport
  diff, to fix a failing cherry-pick before it is dropped. The repair may not
  delete changed tests (`test_path_patterns` says which paths are tests).
- **Test adaptation.** A test file the target branch cannot build or run is
  never silently discarded. A separate sandboxed pass must port its intent into
  an existing branch-native test and revalidate the full candidate, or the
  candidate fails closed. Added files that belong only to the unsupported
  harness are given as context and removed in the same validated commit;
  independently mapped tests stay unchanged.

## Poll

The daily sweep tops a rolling PR up and then waits for the next day, so a
merged sweep PR would sit empty until then. `backport-poll.yml` closes that
gap: it runs hourly, polls immediately and once more 30 minutes later inside
the same runner, and for each `{repo, branch}` runs the sweep only when no
sweep PR is open for that branch.

```text
poller.py
  -> reads repos.yml and fans out one job per {repo, branch}
  -> find_existing_pr(...) -> open sweep PR for this branch?
       yes -> skip; a human is reviewing it
       no  -> run_backport_sweep(...) opens a fresh PR
```

The open-PR check is the whole state model: a merge closes the sweep PR, the
next poll tops the board back up, and the new PR locks the branch until it too
merges. Poll, daily sweep and CI follow-up share the
`backport-branch-mutation-{repo}-{branch}` concurrency group with the maximum
pending queue, so they never race for one branch and a later scheduled writer
cannot replace one already waiting. Manual dispatches are one-shot.

## Mark done

`backport-mark-done-poll.yml` reconciles every project board against branch
reality each hour and moves items to "Done" once the backport is actually on
the target branch. It is self-healing: it picks up backports applied by the
sweep, by an earlier run, or by a manual cherry-pick.

## Eligibility

`backport-eligibility.yml` looks at every PR merged to the development branch
in the last three UTC days (so a missed day is covered by the next run) and
comments on the ones that look eligible, naming the release branches that
carry the changed code. It only suggests; putting a PR on a board stays a
maintainer decision. PRs already commented on are skipped.

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
feature that arrived after a release line forked is never offered to that
line. Whatever history cannot answer (a PR too large to blame, an unreadable
branch, a git failure) and a missing or unparseable verdict post nothing for
that PR and fail the run; the other PRs are still judged, and comments go out
after each model batch. A failed run lists the undecided PRs in its job summary.

## Automatic CI follow-up

`backport-ci-followup.yml` covers what the poller leaves alone: an open
bot-owned PR whose own CI has gone red. For each registered branch of a
repository with `automatic_ci_followup: true`, it looks at App-owned open PRs
whose head this engine writes: the `agent/backport/sweep/<branch>` PR first,
then any `backport/`, `agent/backport/` or `agent/ci-fix/` head into that
branch, so a pushed CI fix and a draft fix PR also get another round.

```text
ci_followup.py
  -> find the open bot-owned PRs into <target> whose head branch this engine writes
  -> require App ownership and exact repo/base/branch/current 40-character SHA
  -> wait until all workflow runs for that head are complete
  -> consider Actions jobs only; checks owned by other Apps (DCO, Codecov) are never visible
  -> discard the informational jobs named in ci_followup_ignored_jobs
  -> skip logical jobs already recorded in hidden result-comment markers
     (twin push and pull_request runs for one head count once)
  -> stop after three attempts on the current head
  -> plan one FixTask for the exact prioritized failed job
  -> run it like every other fix (ci_fix/run.py, ci-fix-run.yml)
       backport policy for a backport head, general policy otherwise
       descendant-only commit plus exact-head lease; no rewrite, no DCO sign-off
  -> post the outcome marker; a failed attempt is tried once more, else wait for a new head
```

A refusal retires its job without counting as an attempt. The automated
caller replaces the human-team gate with a machine gate that requires the
exact bot-owned PR, branch and SHA; a moved head cannot be pushed because the
shared push path independently requires the remote head to equal the failed
SHA. The pushed commit starts the PR's normal CI, which with human review
stays authoritative. The fix itself is documented in [ci-fix.md](ci-fix.md).

## Running it

All dispatches target `valkey-io/valkey-ci-agent`.

**Sweep one project in discovery mode** (leave `dry_run` on; set it to
`false` to cherry-pick, push and update the PR):

```bash
gh workflow run backport-sweep.yml \
  --repo valkey-io/valkey-ci-agent \
  --field repo=valkey-io/valkey \
  --field project_number=14 \
  --field dry_run=true
```

**Backport one PR by hand:**

```bash
gh workflow run manual-backport.yml \
  --repo valkey-io/valkey-ci-agent \
  --field pr_url=https://github.com/valkey-io/valkey/pull/3601 \
  --field target_branch=9.0
```

**Revert one commit on a sweep branch** (the branch must start with
`agent/backport/sweep/`):

```bash
gh workflow run manual-revert-commit.yml \
  --repo valkey-io/valkey-ci-agent \
  --field repo=valkey-io/valkey \
  --field branch=agent/backport/sweep/9.0 \
  --field commit_sha=<sha>
```

**Reconcile the boards now** (always live; preview with the local command
below):

```bash
gh workflow run backport-mark-done-poll.yml \
  --repo valkey-io/valkey-ci-agent \
  --field repo=valkey-io/valkey
```

**Run the CI follow-up now:**

```bash
gh workflow run backport-ci-followup.yml \
  --repo valkey-io/valkey-ci-agent \
  --field repo=valkey-io/valkey \
  --field project_number=14
```

**Re-cover an eligibility window** (manual dispatch defaults to `dry_run=true`,
which reports verdicts in the job summary without commenting):

```bash
gh workflow run backport-eligibility.yml \
  --repo valkey-io/valkey-ci-agent \
  --field date=2026-09-16 \
  --field days=7 \
  --field dry_run=true
```

Both backport workflows upload result JSON and `agent-evidence` artifacts when
available.

### Locally

Check matrix generation:

```bash
python -m scripts.backport.matrix --registry repos.yml --repo valkey-io/valkey
```

Discover a branch's candidates without cherry-picking or pushing:

```bash
python -m scripts.backport.sweep \
  --registry repos.yml --repo valkey-io/valkey --branch 9.0 \
  --target-token "$GITHUB_TOKEN" --discover-only --verbose
```

Preview the mark-done reconciliation (drop `--dry-run` to apply; omit
`--target-branch` for every branch of the repo):

```bash
python -m scripts.backport.mark_done \
  --registry repos.yml --repo valkey-io/valkey --target-branch 9.0 \
  --target-token "$GITHUB_TOKEN" --dry-run --verbose
```

Judge one day's merged PRs for eligibility (needs Bedrock credentials):

```bash
TARGET_TOKEN=$(gh auth token) python -m scripts.backport.eligibility \
  --repo valkey-io/valkey --date 2026-09-16 --days 1 --dry-run
```

The single-PR command is write-capable once it reaches push and PR creation.
Use it only with an intended target repository and token:

```bash
BACKPORT_GITHUB_TOKEN="$GITHUB_TOKEN" python -m scripts.backport.main \
  --registry repos.yml --repo valkey-io/valkey \
  --pr-number 3601 --target-branch 9.0 --verbose
```

## Configuration

Everything per repository lives in [`repos.yml`](registry.md). The workflows
use the shared secrets and OIDC role in [setup.md](setup.md).

## Internals

```text
sweep.py (daily cron or manual dispatch)
  -> reads repos.yml and fans out one job per {repo, branch}
  -> discovers PRs from each branch's GitHub Project board
  -> for each registered release branch:
      source_plan.py     -> resolves merge/squash sources; rejects rebase merges
      candidate_apply.py -> applies one candidate and resolves conflicts with Claude Code
      sweep_validation.py-> build, rules, profile, generated files; repair and adaptation
      pr_creator.py      -> opens/updates the PR on the upstream repo
```

Entry points:

- `scripts/backport/sweep.py` - daily sweep across registered repos and branches
- `scripts/backport/poller.py` - hourly poll that sweeps a branch only when no sweep PR is open
- `scripts/backport/mark_done.py` - board reconciliation
- `scripts/backport/ci_followup.py` - current-head CI follow-up for open bot-owned PRs (plan and report)
- `scripts/backport/eligibility.py`, `eligibility_evidence.py` - eligibility comments and the blame evidence behind them
- `scripts/backport/main.py` - single-PR backport (manual dispatch)
- `scripts/backport/revert_commit.py` - revert one commit on a sweep branch
- `scripts/backport/onboarding.py` - open the registry issue and PR for a new GA line (called by [release automation](release-automation.md))
- `scripts/backport/matrix.py` - GitHub Actions matrix generation from `repos.yml`
- `scripts/backport/registry.py` - typed registry loader and validation
- `scripts/backport/conflict_resolver.py` - the conflict-resolution prompt and result handling
- `scripts/backport/missing_test_adaptation.py` - test adaptation for unsupported harnesses
- `scripts/backport/sweep_*.py` - typed sweep results, git workspace operations, GitHub PR operations, GraphQL access, validation command execution, and Markdown reporting
