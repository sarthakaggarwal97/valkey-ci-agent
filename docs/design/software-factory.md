# A software factory for Valkey

*From a labelled issue to a tested pull request.*

Many of us have our own prompts and setup for getting a model to write
code, each with its own idea of when the result is good enough.
That works for one person. It doesn't work for a project: nobody can see
the recipe, improve it for everyone, or say how often it is right. This
post proposes one shared recipe for Valkey, kept in a repository, reviewed
like code, and measured in one place.

```mermaid
flowchart TD
    A["Bug report"] -->|"a committer applies needs-agent-fix"| B["Intake: who labelled it, and is it eligible?"]
    B -->|"no"| X["Label removed, with the reason"]
    B -->|"yes"| C["Phase 1: write a test that fails"]
    C -->|"no test fails the right way"| D["Comment: what it found, and what it could not reproduce"]
    C --> E["Phase 2: write a fix the test passes, then checks and a second review"]
    E -->|"no fix holds up"| F["Comment: the failing test and the analysis"]
    E --> G["Draft PR from the bot's fork"]
    G --> H["Upkeep: fix its own failures, rebase on conflict, mark it ready"]
    H --> I["Committers review and merge, as for any PR"]
```

The goal isn't speed: in a sample of recently closed Valkey bugs, a person
had opened a fix PR for most of them within a day. Review is the slow part
of our process, so the design spends as little review time as possible.

`valkey-ci-agent` already runs a CI fix bot that takes a failed CI job, has
Claude propose a fix, verifies it, and posts it. This reuses that engine
and most of the code around it; [what it reuses](#what-it-reuses) maps
each part. What changes is the input: a bug report, which anyone can
write, instead of a CI log.

## Three rules

1. **Nothing the bot writes is pushed until code has built it and run its
   test.** A change that can't be verified is posted as a comment, or not
   at all.
2. **The bot never merges or approves.** Committers review and merge its
   PRs like any other.
3. **Only committers can direct it.** Applying the label and every command
   on a bot PR need write access to `valkey-io/valkey`, checked live. The
   trigger is a label rather than a comment because a label is a queue
   anyone can see, waits when the bot is busy, and cancels when removed.

## Evidence first: a failing test

A CI fix starts from evidence: a job that failed. A bug report is only
someone's description, so the bot's first job is to produce the evidence
itself, a test that fails because of the bug. The run has two phases.

```mermaid
flowchart TD
    A["Claude writes a test and names it"]
    B["Code checks the change:<br/>test files only, test declared,<br/>no harness edits, no crashing servers"]
    C["Build the pinned commit<br/>run the test three times"]
    R["Output goes back to Claude"]
    N(["After five attempts: comment<br/>with what it could not reproduce"])
    P2(["Phase 2"])
    S(["Stop and publish nothing"])
    A --> B
    B -->|"fails a check"| R
    B -->|"passes"| C
    C -->|"fails 3 of 3 on its own assertion"| P2
    C -->|"does not fail the right way"| R
    C -->|"crash on a CRASH report"| P2
    C -->|"crash nobody expected"| S
    R -->|"attempts left"| A
    R -->|"none left"| N
```

**Phase 1: reproduce.** The test may add files, fixtures or test modules
under `tests/` or `src/unit/`, but not touch the harness (`tests/support/`,
`tests/helpers/`, `test_helper.tcl`, the unit-test Makefile). "Its own
assertion" means the runner reports the failure for that test by name: an
`[err]` from runtest with no `[exception]` or timeout, or a `[  FAILED  ]`
from the unit-test binary. A test that fails only because it calls a
command the fix hasn't added yet doesn't count.

Valkey fixes crashes in public, and so does the bot: for a `[CRASH]`
report, a crash is the expected failure. A committer who thinks a bug may
be a vulnerability follows `SECURITY.md` instead of applying the label. A
crash nobody expected stops the run, and the bot publishes nothing about
it.

**Phase 2: fix.** The phase 1 test is frozen: code records its hash and
checks it after every build, so the fix can't weaken what it is judged
against. Claude may change other existing tests, usually one that
asserted the buggy behaviour, but only by listing each change and why.
Then code checks that:

- the test passes ten times in a row, and once on an AddressSanitizer build
  with no report;
- every changed test file passes whole;
- `-Werror`, `clang-format` and the generated-file check pass;
- nothing under `.github/` or `deps/` changed, no command was added, and
  the patch is small enough to review in one sitting;
- a second, read-only Claude session approves it against the checklist
  below.

A failure goes back to Claude once. If nothing holds up, the bot still
posts the failing test, which helps whoever picks up the bug.

These checks are necessary, not sufficient. In the sample I read, one
merged fix passed all its tests and a reviewer still found three holes in
it. The checks keep PRs that aren't worth a reviewer's time from reaching
one.

Detector issues, the `test-failure` issues the bot's Test Failure Detector
files after a Daily CI failure, keep going through the existing engine
with the failed job as evidence. They are mostly flaky tests, which don't
fail on demand, so the engine can only show the test passing repeatedly
with the fix. Their PRs say that this is a weaker claim.

## Code decides, Claude helps

Code works out what to act on, checks everything Claude hands back, and
makes every commit and push. Claude is used only where reading and
understanding code is the task. A run is the CI fix bot's three jobs, and
only the outer two can reach GitHub.

```mermaid
flowchart TD
    I["Bug report: anyone can write it"]
    subgraph Plan["Plan job: read and comment token"]
        P["Check who labelled it<br/>pin the issue and the unstable commit"]
    end
    subgraph Fix["Fix job: no GitHub token"]
        C["Claude: edits files, no shell"]
        T["Builds and tests: separate accounts,<br/>loopback only, read-only build"]
    end
    subgraph Publish["Publish job: write token"]
        U["Recheck the target<br/>filter the text, scan for secrets"]
    end
    O["PR from the bot's fork,<br/>treated like an outside contributor's"]
    I -->|"a committer applies the label"| P
    P -->|"links never fetched<br/>only committers' comments are direction"| C
    C <--> T
    T -->|"only what passed every check"| U
    U --> O
```

A hostile bug report may try to steer Claude, and the design assumes that
sometimes works. So the issue is pinned once a committer has read it, the
fix job has no token, and tests can't reach the network or change the
built server. Publish strips mentions, extra closing keywords and links
from everything the model wrote.

The bot keeps no database and no state of its own. Everything it needs to
remember lives on GitHub:

- the label is the queue;
- the bot's status comment on the issue records what it did, which
  revision it read, and how many runs it has made;
- a marker on the PR counts its CI fixes;
- repository variables hold the kill switch and the rollout step.

Every pass rereads that state, so a run that dies leaves nothing to clean
up, and repeating a pass can't push or post twice. The markers are
bookkeeping, not a security boundary: committers can edit comments.

The PRs come from a fork owned by a machine user. Today the bot pushes
backport branches to `valkey-io/valkey` itself, where CI runs the moment a
branch is pushed, before anyone has read it, in jobs holding the Codecov
token and CodeQL's write permission. That's acceptable for cherry-picks of
reviewed commits, not for code written from a stranger's bug report. A
fork PR gets what any outside contributor's PR gets, and Valkey's own
`AGENTS.md` says agents should never push upstream. The fork belongs to a
user account because GitHub won't let committers push fixups to an
organization-owned fork. The bot's token on `valkey-io/valkey` asks only
for pull request and issue write, so it can't merge.

## Meeting the reviewers' standard

No check can guarantee a fix is the one a committer would have written,
and the reviewer session is the same model reading its own kind of work.
Four things raise the floor, and recurring review comments feed back into
them.

```mermaid
flowchart TD
    G["Valkey's guidelines"] --> R["The recipe: prompts, lint, checklist"]
    E["Recent fixes and their reviews"] --> R
    R --> PR["Bot PRs"]
    PR --> RV["Committers review"]
    RV -->|"a comment that keeps coming up"| CH["Change the lint, the checklist,<br/>the bot's guide or AGENTS.md"]
    CH -->|"replayed against past bugs first"| R
```

**Valkey's guidelines:** the ones the CI fix bot already hands its agent:
`AGENTS.md`, `DEVELOPMENT_GUIDE.md`, `tests/README.md` and
`src/unit/README.md`, read from the default branch so a PR can't rewrite
the rules it is judged by. Not `.github/instructions/`: those were added
for Copilot, derive from `DEVELOPMENT_GUIDE.md`, and have mostly not
changed since, and a stale copy is worse than none.

**Recent fixes and their reviews:** the two or three latest merged fixes to
the files being changed, with the review comments they got. Guidelines say
what the standard is; these show it.

**A lint** for what a machine can decide, such as waiting with
`wait_for_condition` instead of a fixed `after`, each rule citing its
guideline.

**A checklist** for what it can't. The reviewer session rejects a change
that isn't the smallest fix for the cause, doesn't match the code around
it, tests something easier than the issue claims, leaves non-obvious
behaviour unexplained, would be hard to backport, or changes an existing
test without a reason that holds.

Advice that applies to every contributor should move from the bot's own
guide into `AGENTS.md`, where everyone's tools read it. For modules, each
repository gets its own profile of guidelines, test runners, build
commands and lint. `valkey-json` and `valkey-bloom` would first need an
`AGENTS.md`, written with their maintainers.

## The pull request

```mermaid
stateDiagram-v2
    state "Hands off" as HandsOff
    [*] --> Draft: a run passes
    Draft --> Draft: its own change fails CI, the bot fixes it (at most twice)
    Draft --> Draft: merge conflict with unstable, the bot rebases
    Draft --> Ready: every workflow green
    Draft --> HandsOff: a committer reviews or pushes, or agent-hands-off
    Ready --> HandsOff: a committer reviews or pushes, or agent-hands-off
    HandsOff --> HandsOff: the bot acts only on a committer's command
    HandsOff --> Merged: committers approve and merge
    Ready --> Closed
    HandsOff --> Closed
    Merged --> [*]
    Closed --> [*]
```

The PR's body opens with a short summary meant to become the squash commit
message. The rest sits under `<details><summary>AI generated
details</summary>`, as Valkey PRs already do: the root cause, each change
to an existing test and why, what code verified, and what isn't
established. The bot's commits carry no DCO sign-off; as on its backport
PRs today, a committer approves the DCO check by hand when merging.

Until a committer reviews it, the bot looks after the PR in two ways, and
nothing else.

**It fixes only failures its own change caused.** When a CI job goes red,
the bot checks the same job on the latest `unstable`. If it's red there
too, the failure isn't the PR's, so the bot names the job on the PR and
leaves it. Otherwise it hands the job to the CI fix bot, at most twice, and
the fix may only touch files the PR already changes. Any other failing
test, a committer can hand to the bot with `@valkeyrie-ops fix`, the
command that exists today.

**It rebases only on a merge conflict.** A branch that is merely behind
`unstable` is left alone: `unstable` doesn't require it, and every push
reruns CI. When the PR conflicts, the bot replays its change onto the
current `unstable`, resolves the conflicted files the way the backport
sweep resolves cherry-pick conflicts, reruns every check, and pushes only
if they pass.

Once a committer has reviewed or pushed, the bot pushes only when a
committer asks, with `@valkeyrie-ops fix` or, from step 4, `address`. A
conflict after review is only reported: the committer resolves it, as on
any contributor's PR, so the bot never rewrites code under a review. If it pushes after an approval, it dismisses the approval,
because `unstable` doesn't and an approval should mean the code that was
read.

## Rolling it out

Each step has to earn the next.

1. **Replay.** Run the recipe on 40 recently closed bugs, writing nothing to
   Valkey, and count how often a committer would have merged the result.
2. **Comments.** The label turns on, but the bot only comments with its
   test and patch.
3. **Pull requests.** Draft PRs, at most five open at a time. A revert, or a
   run of PRs closed unmerged, stops new ones.
4. **Review loop.** `@valkeyrie-ops address` applies a committer's review
   comments in one commit, rerunning every check. It never runs unprompted.

## What it reuses

Most of the factory already exists in `valkey-ci-agent`. The table maps
each part to what it reuses, and what is new.

| Part | Reuses | New |
|---|---|---|
| Intake | The detector-issue dispatcher and `ci-fix-issues.yml`, which already queue one run per issue, two at a time; the live permission check in `ci_fix/gate.py` | Reading the label event, checking the labeller's role instead of team membership |
| The run | `ci-fix-run.yml`'s plan, fix and publish jobs; the CI fix engine and its two Claude sessions; the separate system account in `common/isolation.py`; the test loops in `ci_fix/verification.py` | A way into the engine without a failed CI job; the reproduce phase; crash detection; loopback-only networking and a read-only build |
| Publishing | `ci_fix/publish.py` and the leased push in `ci_fix/push.py`; the backport registry's `push_repo`, which already pushes to a fork owned by another account | Filtering the model's text and scanning for secrets |
| Fixing CI | The backport CI follow-up, which already watches the bot's PRs and hands one failed job at a time to the CI fix bot; `@valkeyrie-ops fix` for anything a committer wants fixed | Comparing with the same job on `unstable`; following PRs into `unstable`; pushing to a fork head, where `fix` only suggests a patch today |
| Rebasing on conflict | The backport sweep's cherry-pick and Claude conflict resolution in `backport/conflict_resolver.py` | Running it under the fix job's containment |
| Review loop | The release-notes watcher's review-thread reading and replies in `release_notes/review.py` | Applying comments to code rather than one notes file |
| Quality | The bot's guide in `ci_fix/guides/` and the Valkey guidelines it already hands the agent; the replay eval on the `ci-fix-replay-eval` branch | Recent fixes with their reviews, the lint, the checklist, issue cases in the replay |
| State | GitHub only: labels, marker comments and run names, as every other workflow here | Nothing |

## Open questions

- A fork, as argued above, or branches in `valkey-io/valkey` like
  backports?
- Who funds the model spend? The default model requires Bedrock's provider
  data sharing, so the provider keeps what a run reads and writes.
