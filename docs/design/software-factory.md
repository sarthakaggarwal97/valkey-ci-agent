# A software factory for Valkey

*From a labelled issue to a pull request a committer is willing to own.*

Status: proposal, not built yet. Author: Sarthak Aggarwal. Draft of
11 November 2026, written against `valkey-ci-agent`'s `main` at
`b1b87c6f`.

If you are on the TSC, the first four sections are the ones to read:
[the idea](#the-idea), [what we need you to decide](#what-we-need-the-tsc-to-decide),
[how it rolls out](#how-it-rolls-out) and
[what to expect](#what-to-expect). Everything after them is the detail
someone needs in order to build it.

## The idea

A committer looks at a bug report, decides it is a real bug the bot can
take, and puts the label `needs-agent-fix` on it. The bot writes a test
that fails because of the bug, then writes the change that makes the test
pass. If both hold up, it opens a pull request that says `Fixes #N`.

That committer owns the pull request from then on, the same way they would
own one they had written. They answer the reviewers, a second committer
approves it, and the owner merges it with their own sign-off. While the PR
waits, the bot keeps it green. Later in the rollout it also applies review
comments when a committer asks it to. It never merges anything.

```mermaid
flowchart TD
    A["Bug report"] -->|"a committer applies needs-agent-fix"| B["Intake: who labelled it, and is it eligible?"]
    B -->|"no"| X["Label removed, with the reason"]
    B -->|"yes"| C["Phase 1: write a test that fails"]
    C -->|"no test fails the right way"| D["Comment: what it found, and what it could not reproduce"]
    C --> E["Phase 2: write a fix the test passes, then checks and a second review"]
    E -->|"no fix holds up"| F["Comment: the failing test and the analysis"]
    E --> G["Draft PR from the bot's fork"]
    G --> H["Upkeep: keep it green, mark it ready"]
    H --> I["A second committer approves"]
    I --> J["The owner squash-merges, with their own sign-off"]
```

### Why do this at all

Many of us now have our own prompts and our own setup for generating code,
and each has a different idea of when the result is good enough. The point
of this is to stop relying on individual setups and settle on one on
purpose: one recipe, kept in a repository, reviewed like code, reading
Valkey's own guidelines, running the same checks every time, with its
results measured in one place. A standard process is what lets us improve
it.

It is not about speed. In a sample of recently closed bugs, a person had
opened a fix PR for most of them within a day. The slow part of Valkey's
process is review, so most of this design is about spending as little
review time as possible.

### Three rules

These hold everywhere in the design. If a later section seems to bend one,
the later section is wrong.

1. **Nothing the bot writes is pushed until code has built it and run its
   test.** A change that cannot be verified is posted as a comment, or not
   at all. The one exception is a committer telling the bot to drop the
   test from a PR; it still builds and runs everything that is left.
2. **Every bot PR has a person who owns it,** and the bot never merges or
   approves.
3. **Only committers can direct it,** on issues and on its own PRs.

### What it costs

For each bot PR: an owner who reads it as if it were their own and answers
for it; a second committer who reviews and approves it; a committer
approving the DCO check by hand, as we already do on the bot's backport
PRs; and possibly a committer approving its workflow runs, which we still
have to check. To keep that cost down, the bot hands its PRs over green,
never has more than five open, and stops pushing on its own as soon as a
committer has reviewed.

Building it is a real project, not a configuration change. The first three
steps are each a substantial piece of work; [the work](#the-work) lists
what each one needs.

## What we need the TSC to decide

This creates two new roles, the owner of a bot PR and the operator of the
factory, so the TSC may want to treat it as a major decision under
GOVERNANCE.md. That is the TSC's call.

| # | Question | What I propose | Needed before |
|---|---|---|---|
| 1 | **Certification.** A bot cannot sign the DCO. | The owner certifies the change as an AI-assisted contribution of their own: when they squash-merge, they add their `Signed-off-by` and an `Assisted-by:` line naming the model. The squash commit will probably name the bot as its author, so the owner signs off as someone who is not the author, and `CONTRIBUTING.md` says Valkey does not accept anonymous contributors. I'd like the TSC to say whether the owner's sign-off is enough, with Linux Foundation counsel if it wants. If not, we stop at step 0, because step 1 already invites people to reuse the bot's patches. | Step 1 |
| 2 | **Where the bot's branches live.** | In principle, in a fork, so the bot's PRs get exactly the trust an outside contributor's get. For committers to be able to push to it, the fork has to belong to a user account: a machine user, whose only credential is a fine-grained token limited to that fork, kept as a secret in `valkey-ci-agent` and rotated every 90 days. I'll propose who owns the machine user before step 2, once the checks in [verify first](#verify-first) are done. | Now in principle; the owner before step 2 |
| 3 | **Who may apply the label.** | Committers: anyone with write access or higher. Applying the label is also the security call. If you think a bug might be a vulnerability, you follow `SECURITY.md` instead, exactly as you would for a fix you wrote. | Step 1 |
| 4 | **Who runs it.** | I operate it, with a second operator named before step 1. Admins of `valkey-io/valkey-ci-agent` can turn it off. Any committer can cancel one issue by removing the label, or stop the bot pushing to one PR on its own with the label `agent-hands-off`. Changes to the recipe are PRs to `valkey-ci-agent`, and each needs approval from a Valkey committer. | Step 1 |
| 5 | **Spend, the model vendor, and data.** | The bot uses Claude on Bedrock through the AWS account the agent already uses, which is funded by **[to be filled in by the author before the vote]**. The model runs with Bedrock's provider data sharing on, because the default model is not offered without it, so the provider receives everything a run reads and writes, including any test that crashes the server. Step 0 runs under a budget the TSC sets and records the cost of every run. Before step 1 the TSC sets a monthly ceiling for factory runs only; the bot starts no run that could take the month past it, counting each run at the most expensive run step 0 saw. | Step 0 |
| 6 | **Go or no-go.** | Step 0 replays 40 closed bugs. A case counts as publishable only if the bot's checks pass and a committer who did not write the original fix says they would own the bot's PR. I propose going on if at least one case in five is publishable. | The pass mark before step 0; the decision after it |
| 7 | **A private channel for crashes nobody expected.** | When a run hits a crash or sanitizer report the owner did not expect, the bot puts the test in a draft security advisory on `valkey-io/valkey`, which only the owner and the security team can see, and says nothing more in public than it would for any failed run. That needs the App to be allowed to write repository security advisories. Without it, the bot discards the test. | Step 1 |

Every other limit and threshold in this document is a starting value. I'll
set them, and report on how they hold up.

## How it rolls out

Each step is switched on by a repository variable, and each one has to
earn the next.

```mermaid
flowchart TD
    S0["Step 0: replay past bugs<br/>nothing written to Valkey"]
    S1["Step 1: comments on issues<br/>no PRs"]
    S2["Step 2: draft PRs, kept green"]
    S3["Step 3: the review loop"]
    S0 -->|"the TSC says go"| S1
    S1 -->|"its gate, and the TSC's yes"| S2
    S2 -->|"its gate"| S3
```

**Step 0, the replay.** The recipe runs against past bugs, and nothing is
written to `valkey-io/valkey`. It tells us how often the bot would have
produced something worth publishing, and what that costs.

**Step 1, comments.** The label works on `bug` issues and on the
detector's `test-failure` issues. A run posts its test, its analysis and
its patch as a comment on the issue, with no branch and no PR. This alone
may be worth keeping for a long time: a failing test on a bug nobody has
looked at helps whoever picks it up. It moves on when, within 90 days,
there are at least ten such comments, and for at least half of them either
a person's later fix confirms the bot's test (it fails before that fix and
passes after it) or a committer other than the owner marked the comment
useful with a 👍. Then the TSC is asked again before step 2, since step 2
is the first time the bot opens PRs into Valkey.

**Step 2, pull requests.** A labelled run that passes opens a draft PR,
and upkeep keeps it green and marks it ready. It moves on when the
[success numbers](#measuring-it) hold over the first ten PRs.

**Step 3, the review loop.** A committer can ask the bot to apply review
comments, or to bring a PR up to date with `unstable`. That is described
in [step 3](#later-step-3-the-review-loop).

If a step misses its gate, it stays where it is, I write up why, and the
TSC decides whether to stop. A revert of a bot PR, or three bot PRs in a
row closed unmerged, stops new PRs until the operator clears the hold; see
[limits and rollback](#limits-and-rollback).

Only a labelled run opens a PR, because only a labelled run has an owner.
The detector keeps posting its automatic patch comments, and its existing
option to open draft PRs without an owner is retired.

## What to expect

For this document I read two samples of past work. Nothing was run, so
"would fail first" below is a judgement from reading the diffs. Sampling
closed issues also favours bugs that were fixed quickly. These samples
were used only to estimate, not to tune anything.

| | |
|---|---|
| Of the 32 PRs that fixed the 40 most recently closed `bug` issues: how many came with a test | 26 |
| … how many had a test that would plausibly fail first on one Ubuntu machine | at most 21 |
| … how many a person opened within 24 hours of the issue | 23 |
| … median days from opening to merge | 6.5 |
| Of about 20 fix PRs merged between 23 October and 10 November: crashes, assertions or leaks, fixed in public with a reproducing test | 8 |
| Open PRs in `valkey-io/valkey` today, and how many await review | 448 and 411 |
| Open `bug` issues today, unassigned, with no linked PR | 20 |
| Open `test-failure` issues today, unassigned, with no linked PR | 23 |

What I take from this:

The pool is small. The bot only takes labelled issues, and a lot of
Valkey's fixes arrive as a PR with no issue behind them. It also needs a
bug that one test on one Linux machine can show, which rules out leaks,
other platforms, multi-node timing, performance and redesigns.

The gain is not volume. It is the shared recipe, fixes for bugs nobody has
picked up, and failing tests that are useful to a person even when the
bot's fix is not.

---

## Words used here

A **committer** is anyone with write access or higher to
`valkey-io/valkey`, which is how GOVERNANCE.md uses the word; TSC members
are committers too. The **owner** of a bot PR is whoever it is assigned
to: first the committer who applied the label, then any committer who
takes it over. A **run** is one pass of the pipeline for one issue: plan,
fix and publish. One execution of a test is a *test run*, and one GitHub
Actions run is a *workflow run*. A **detector issue** is a `test-failure`
issue that the bot's Test Failure Detector filed after a test failed in
Daily CI.

A **crash** is a server process, or the unit-test binary, ending on
SIGSEGV, SIGBUS, SIGILL, SIGFPE or SIGABRT (a failed `serverAssert` raises
SIGABRT), or an AddressSanitizer report. LeakSanitizer is switched off,
since leaks are out of scope. A timeout is an ordinary failure, not a
crash.

## How a run works

The bot keeps no database. Every stage reads the current state from
GitHub and acts on that.

### Picking up the label

Twice an hour a scheduled job lists the open issues that carry
`needs-agent-fix`. For each new one it reads the `labeled` event in the
issue's timeline, which records who applied the label, and checks that
person's role on the repository there and then. They need `write`,
`maintain` or `admin`. The event's id is the key, so each application of
the label is handled exactly once.

The bot takes the label off again, and replies saying why, when:

- the person who applied it has any other role, is a bot or an App, or the
  check fails for any reason at all;
- the issue is closed or locked;
- it is neither a detector issue nor labelled `bug`;
- someone is assigned, or an open PR says it `Fixes`, `Closes` or
  `Resolves` the issue;
- it carries `major-decision-*`, `breaking-change`, `pending-refinement`,
  `not-a-bug`, `help wanted` or `good first issue`. The last two are meant
  for people;
- someone who is not a committer edited the title or body in the hour
  before the label went on, or after it and before the run read the
  issue. A reporter must not be able to change what the bot acts on after
  a committer has read it;
- three runs have already started for it since it was last opened (an
  automatic restart does not count);
- starting it could take the month past the spending ceiling.

Otherwise the issue waits with the label on. It starts once fewer than two
runs are in progress and, from step 2, fewer than five bot PRs are open.
Detector runs share those two places, and labelled issues go first.

The bot posts one status comment and edits it as it goes, with a link to
the revision of the issue it read. Removing the label cancels the run: the
next intake pass cancels its workflow run, and the publish step checks for
the label in any case, posts nothing, and marks the status comment
cancelled. Whether someone in the thread has already offered to take the
issue is for the committer applying the label to judge.

### What Claude gets to read

It reads the issue's title and body, and the comments written before the
label went on that have not been edited since. All of that is marked as
untrusted text, except that comments by committers are marked as
direction. It never follows links. It also reads merged commits that
mention the issue or touch the files it names, and the guidelines and
recent reviewed fixes listed under
[meeting the reviewers' standard](#meeting-the-reviewers-standard).

### Three jobs, and who holds what

A run is the same three jobs the CI fix bot uses today. The middle one,
where Claude works, has no GitHub token at all.

```mermaid
sequenceDiagram
    participant GH as GitHub
    participant P as Plan
    participant F as Fix
    participant C as Claude
    participant U as Publish
    P->>GH: read issue and roles, post status (read and comment token)
    P->>F: pinned plan
    Note over F: no GitHub token
    F->>C: issue, guidelines, history
    C->>F: test, then fix (edits only)
    F->>F: build and test as another account
    F->>U: verified result
    U->>GH: recheck, then comment or push (write token)
```

**Plan** pins down the issue as it was read, the owner, and the commit of
`unstable` to work on, and posts the status comment. **Fix** does the
work. **Publish** checks the destination again, and only then writes.

A detector issue goes through the existing engine, with its failed CI job
as the evidence. Detector failures are mostly flaky tests, and flaky tests
don't fail on demand. So for them the engine shows that the test passes
repeatedly with the fix, up to 40 times and fewer for valgrind or slow
tests, and does not try to show it failing first. Everything from here on
is about `bug` issues, where a failing test is the only evidence that
there is a bug at all.

### What counts as a test

A named test case that one of Valkey's runners reports by name:
`./runtest` for the Tcl tests under `tests/unit/` and `tests/integration/`,
`./runtest-moduleapi` for `tests/unit/moduleapi/`, and the unit-test binary
for `TEST` and `TEST_F` cases under `src/unit/`. The name has to be
written out literally, not built while the test runs. Sentinel tests are
out of scope.

### How things are built

A separate account does the compiling, including the test modules and the
TLS test certificates. The tree is then made read-only to the account that
runs the tests, apart from a fresh `tests/tmp`, so a test cannot change the
server between one test run and the next. A unit test lives inside the
unit-test binary, so that is rebuilt on every attempt. Builds and test
commands run with no network other than loopback. After every build, code
checks that the files it froze (below) still have the hashes it recorded.

### Phase 1: a test that fails

```mermaid
flowchart TD
    A["Claude writes a test and names it"]
    B["Code checks the change:<br/>test files only, test declared,<br/>no harness edits, no crashing servers"]
    C["Build the pinned commit<br/>run the test three times"]
    R["Output goes back to Claude"]
    N(["After five attempts: comment<br/>with what it could not reproduce"])
    P2(["Phase 2"])
    S(["Stop: see Crashes"])
    A --> B
    B -->|"fails a check"| R
    B -->|"passes"| C
    C -->|"fails 3 of 3 on its own assertion"| P2
    C -->|"does not fail the right way"| R
    C -->|"crash the owner expected"| P2
    C -->|"crash nobody expected"| S
    R -->|"attempts left"| A
    R -->|"none left"| N
```

Claude writes a test. It may add test files, fixtures or test modules
under `tests/` or `src/unit/`, and it tells us the test's name. It may not
change the harness: `tests/support/`, `tests/helpers/`,
`tests/test_helper.tcl`, `src/unit/Makefile`, or the scripts that generate
unit-test files. Code checks all of that, checks that an added line
declares the test (`test {name}` in Tcl, `TEST` or `TEST_F` in C++), and
checks that the test does not crash or kill servers on purpose: no
`exec kill`, no `DEBUG` subcommand that crashes or stops the server
(`DEBUG SEGFAULT`, `DEBUG ASSERT` and the like), and no `abort()` in a test
module.

Then code builds the pinned commit and runs the named test in three
separate test runs. The test counts only if all three fail in the right
way. The runner has to report the failure for that test by name: an
`[err]` from runtest, with no `[exception]` and no timeout, or a
`[  FAILED  ]` line from the unit-test binary. The failure must not just be
something the test uses that does not exist yet. Code catches the plain
cases, unknown commands and subcommands and build failures, and the
reviewer session is told to look for the rest. And nothing may have
crashed, which code checks from the exit status of the unit-test binary
and from the server logs runtest keeps for the test (`--dump-logs`), for
Valkey's crash report and for sanitizer reports.

A crash is fine when the owner labelled the issue knowing it was one,
meaning the title starts with `[CRASH]` (the prefix Valkey's crash-report
template uses), or an earlier run on it stopped on a crash. In that case
the test runs on an AddressSanitizer build, and a crash there counts as
the failure, including the `[exception]` runtest reports for it. Any other
crash, in either phase, stops the run; see [crashes](#crashes).

If the test doesn't count, the output goes back to Claude. It gets five
attempts in all.

### Phase 2: a fix that the test passes

Claude now writes the fix. Code has recorded a hash of every file phase 1
wrote and of the harness, and none of them may change. Claude may change
other existing tests, but only by listing each change and saying why. The
usual reason is a test that asserted the buggy behaviour. The reviewer
session checks each one, and the PR body lists them.

Then code runs six checks, in this order:

1. **Green.** The test passes ten times in a row on the default build,
   after one build. If ten won't fit in the time left, the attempt fails.
2. **Memory.** The test passes once on an AddressSanitizer build with no
   report, and code checks the binary really was built with it.
3. **Neighbours.** Every test file phase 1 added to or phase 2 changed runs
   whole, once.
4. **The cheap parts of CI.** A default build with `-Werror`,
   `clang-format` 18.1.8, the generated files `ci.yml` checks are up to
   date, and the lint described under
   [meeting the reviewers' standard](#meeting-the-reviewers-standard).
5. **Bounds.** Nothing under `.github/` or `deps/`, no new file under
   `src/commands/`, and a patch under 45,000 characters (the limit the bot
   already uses for suggested patches) so the reviewer session can read all
   of it. This keeps PRs focused. It is not a security boundary.
6. **Review.** A separate, read-only Claude session with the same
   guidelines reads the change and the test output, and works through the
   checklist under
   [meeting the reviewers' standard](#meeting-the-reviewers-standard).

If a check fails, the output goes back to Claude once, so the fix gets two
attempts.

As a starting time budget inside the engine's four and a half hours: 10
minutes for each phase 1 session, 45 for each phase 2 session, 15 for
each review session, and the rest, about two hours, for builds and test
runs. Step 0 will tell us what is actually needed.

These checks are necessary, not sufficient: in the sample, one merged fix
passed all its tests and a reviewer still found three holes in it. They
keep PRs that aren't worth a reviewer's time from reaching one.

### How a run ends

| What the run found | What the bot does |
|---|---|
| A test that counts and a fix that passes | At step 1, posts both on the issue. At step 2, opens a draft PR. |
| A test that counts, but no fix that passes | Posts the test and the analysis |
| No test that counts | Posts the analysis, and what it could not reproduce |
| A crash nobody expected, in either phase | Says the run produced no result; see [crashes](#crashes). No restart. |
| The issue needs a decision, or the fix would be out of bounds | Says which |
| A person took the issue, or the label was removed | Marks the status comment cancelled, and nothing else |
| The runner or the model failed | Restarts once, then says the run produced no result |
| The run ran out of time | Says the run produced no result. No restart. |

Afterwards it removes the label, unless someone already did. Putting the
label back on asks again, up to three runs.

## Meeting the reviewers' standard

No check can guarantee that a fix is the one a committer would have
written. The reviewer session is the same model reading its own kind of
work. What we can do is raise the floor, in four layers, and keep the
owner and the approving committer as the real standard.

**Valkey's own guidelines, all of them.** The bot reads `AGENTS.md`,
`DEVELOPMENT_GUIDE.md`, `CONTRIBUTING.md`, `tests/README.md` and
`src/unit/README.md`, always from the pinned commit of the target
repository and never from a branch the bot wrote. If one is missing, the
run's report says so. It leaves out the files under `.github/instructions/`.
Those were added for GitHub Copilot's reviews, they say they are derived
from `DEVELOPMENT_GUIDE.md`, and most haven't changed since. A stale copy
that disagrees with its source would only mislead. The bot's own guide in
`scripts/ci_fix/guides/` adds policy, not conventions, and any of its
advice that applies to everyone should move into Valkey's `AGENTS.md`,
where every contributor's tools will read it.

**How committers actually write and review these files.** Guidelines say
what the standard is. Examples show it. So the bot also gets the two or
three most recent merged fixes to the files it is changing, with the review
comments those PRs got: what reviewers accepted, and what they pushed back
on.

**Mechanical checks, for whatever a machine can decide.** On top of phase
2's checks, a short lint enforces the rules from the guidelines that
reviewers keep having to repeat. One example is waiting with
`wait_for_condition` instead of a fixed `after`. The lint's rules live in
this repository, and each one cites the guideline it comes from.

**A checklist, for what a machine can't decide.** The reviewer session
rejects a change that isn't the smallest one that fixes the cause, that
doesn't match the surrounding code or brings in a new pattern, that puts
the test in the wrong place or tests something easier than the issue
claims, that leaves non-obvious behaviour without a comment saying why,
that would be hard to backport, or that changes an existing test without a
reason that holds.

When the same review comment keeps coming up on bot PRs, it becomes a
change to the lint, the checklist, the bot's guide or Valkey's
`AGENTS.md`. The replay runs the recipe before that change ships, so a
correction made once doesn't have to be made again.

```mermaid
flowchart TD
    G["Valkey's guidelines"] --> R["The recipe"]
    E["Recent fixes and their reviews"] --> R
    R --> PR["Bot PRs"]
    PR --> RV["Committers review"]
    RV -->|"a comment that keeps coming up"| CH["Change the lint, the checklist,<br/>the bot's guide or AGENTS.md"]
    CH -->|"replayed before it ships"| R
```

### Modules

When the factory takes on the modules, each repository gets its own
profile rather than sharing Valkey's rules. A profile holds that
repository's guideline files, the bot's guide for it, what counts as a
test and which runner runs it, the build and format commands, and the lint.
This document fills in one profile, for `valkey-io/valkey`, but the code
treats it as a profile from the start, so adding a module is configuration
rather than a rewrite.

The modules are not alike. `valkey-search` has an `AGENTS.md`, a
`CONTRIBUTING.md` and an `.agents/` directory. `valkey-json` and
`valkey-bloom` have only a README. A module with no guidelines needs an
`AGENTS.md`, written with its maintainers, before the bot takes its issues.

## The pull request

This is step 2.

The PR comes from the bot's fork, on a branch that is new for every
attempt: `agent/issue/<N>-<workflow run id>-<attempt>`. Because the name is
never reused, a PR that was closed can never come back with its old code.
The branch is deleted 14 days after its PR is merged or closed, so a PR
closed by mistake can still be reopened.

The body starts with a short summary meant to become the squash commit
message. Below it, inside `<details><summary>AI generated details</summary>`,
the way Valkey PRs already do it, come the root cause and what changed,
every change to an existing test and why, what code verified with the
commands and counts, what isn't established, the recipe (this
repository's commit and the model), and a suggested `release-notes` or
`no-release-notes` label. Then `Fixes #N`.

The owner is assigned. Valkey's auto-assign workflow assigns a PR's author,
so the bot swaps itself out for the owner. From then on the owner answers
reviewers' questions and automated review comments as an author would, and
decides whether the PR deserves a second committer's time.

The PR stays a draft until it is green. The bot marks it ready when every
workflow run on its head commit has finished as `success` or `skipped`,
apart from the jobs listed in `ci_followup_ignored_jobs`. That includes the
format check and the Provenance Guard, which compares PRs with Redis
source. If a job is red and the bot doesn't fix it, the bot names it on the
PR, and the PR stays a draft until a committer reruns the job or marks it
ready.

To merge, a committer other than the owner approves, and a committer
approves the DCO check by hand. The owner squash-merges, replacing the
default message with the summary, their `Signed-off-by`, and an
`Assisted-by:` line naming the model. The bot posts that message, ready to
paste, when it marks the PR ready. Its own commits carry neither line; see
[certification](#certification).

### A bot PR's life

```mermaid
stateDiagram-v2
    state "Hands off" as HandsOff
    [*] --> Draft: a run passes
    Draft --> Draft: red CI, the bot fixes it (at most twice)
    Draft --> Ready: every workflow green
    Draft --> HandsOff: a committer reviews or pushes, or agent-hands-off
    Ready --> HandsOff: a committer reviews or pushes, or agent-hands-off
    HandsOff --> Draft: agent-hands-off removed, no review or push yet
    HandsOff --> HandsOff: the bot acts only on a committer's command
    HandsOff --> Merged: second committer approves, owner merges and signs off
    Ready --> Closed
    HandsOff --> Closed
    Merged --> [*]: branch deleted after 14 days
    Closed --> [*]: branch deleted after 14 days
```

### Keeping it green

Twice an hour, one scheduled job reads every open bot PR with a single
query. Until a committer has reviewed a PR, the bot does two things on its
own. It fixes red CI by handing one failed job to the CI fix bot, at most
twice per PR, counted by a marker on the PR. That fix may only touch files
the PR already changes, and never the frozen files or the harness; if it
would need more, the bot names the job and leaves it. A bug-fix PR should
not quietly collect fixes for other people's flaky tests. It also marks the
PR ready, by the rule above. At any stage, it deletes the branch once its
time is up.

### Hands off

The moment a committer submits a review or pushes to the branch, or while
the PR has the label `agent-hands-off`, the bot stops pushing on its own.
It still does what a committer asks: `@valkeyrie-ops fix`, which on bot
PRs needs a committer, and from step 3 `address` and `update`. A review or
a push by a committer makes this permanent; removing the label otherwise
lifts it.

Right before every push the bot reads the PR again: the head commit, the
reviews, the label and auto-merge. If auto-merge is on, it pushes nothing,
whoever asked. If a push follows an approval, the bot dismisses that
approval and says why. `unstable` doesn't dismiss stale approvals by
itself, and an approval should mean the code that was actually read. If
the App isn't allowed to dismiss reviews, it turns the PR back into a draft
instead.

The bot never closes a PR. The weekly report lists bot PRs with no
activity for 30 days, and the owner decides what to do. If the owner leaves
or stops answering, another committer takes over by assigning themselves.
Until step 3 there's no `update`, so if a PR conflicts, the owner closes it
and can put the label back for a fresh run on the current `unstable`.

## Keeping it safe

Most of the containment already exists, because the CI fix bot needed it.
Claude has no GitHub token and no shell. It edits a scratch checkout as a
separate system account. Code builds, tests, commits and pushes, and only
pushes if the branch is still at the commit the run started from. Test
commands get no credentials. The model's own process holds short-lived AWS
credentials for Bedrock, and nothing else. What the model provider keeps is
covered under decision 5.

What is genuinely new is the input. Until now, what the bot pushed came
from CI logs, and on other people's PRs it only ever suggested patches. A
bug report is text anyone on the internet can write, and a hostile one may
well try to steer Claude. The design assumes that will sometimes work, and
limits what can follow from it.

```mermaid
flowchart TD
    I["Issue text: anyone can write it"]
    subgraph Contained["Fix job, with no GitHub token"]
        C["Claude: edits files, no shell"]
        T["Builds and tests: another account,<br/>loopback only, read-only build"]
    end
    Fl["Publish job: filter and secret scan"]
    O["Owner reads it as their own"]
    A["Second committer approves"]
    I -->|"chosen by a committer, frozen once read,<br/>links never fetched,<br/>only committers' comments are direction"| C
    C --> T
    T -->|"only what passed code's checks<br/>and the review session"| Fl
    Fl -->|"PR from a fork, treated as an outside contributor's"| O
    O --> A
```

### The publication filter

Everything the bot posts or commits is filtered first: comments, commit
messages, and the PR's title and body. Code strips `@` mentions other than
the owner's, closing keywords other than the bot's own `Fixes #N`, any
`Signed-off-by` or `Assisted-by` lines, and links. Links here include
images, HTML, and references to other issues or PRs. A patch is posted
exactly as it is, in a code fence longer than any run of backticks inside
it. A pinned secret scanner reads all of it, patch included, and if it
matches anything, nothing is published.

### Why a fork

Today the bot pushes its branches to `valkey-io/valkey` itself. A branch
there runs CI as soon as it is pushed, before anyone has read the diff, in
jobs that hold the Codecov token and CodeQL's write permission. Valkey's
own `AGENTS.md` also says never to push to the upstream repository. A PR
from a fork gets no secrets in the workflows that run its code on
`push` and `pull_request`. Three workflows run on `pull_request_target`
instead: the Provenance Guard and the auto-assign workflow, which don't
run the PR's code, and the benchmark workflow, which builds the PR's code
with a secret when someone adds a benchmark label. That last one is the
same exposure any outside contributor's PR has today.

The token the bot uses on `valkey-io/valkey` for these PRs only asks for
pull request and issue write, and merging needs write on contents. The App
itself still has that permission, for backports, so this rests on how the
workflow asks for its token rather than on anything GitHub enforces.

Committers push fixes to contributors' branches today, and they should be
able to do the same here. GitHub allows that on a fork owned by a user
account, but not on one owned by an organization, which is why the fork
belongs to a machine user (decision 2).

### Crashes

Valkey fixes crashes in public, with a test that reproduces them, and the
bot does the same. The security call is made when the label goes on
(decision 3).

The case the bot guards against is a crash nobody expected: the owner
labelled what looked like a wrong result, and a test crashes the server,
either in phase 1 or when phase 2's AddressSanitizer check finds a memory
error in existing code. Then the run stops, in either phase, and doesn't
restart. In public, the bot says the run produced no result, exactly as it
does when a run fails for ordinary reasons, and the weekly report counts it
the same way. If decision 7 allows, the test goes into a draft security
advisory that only the owner and the security team can see; otherwise it
is discarded. If they judge it isn't a security concern, the owner puts the
label back, and the next run treats a crash as the expected failure.

So that nothing leaks before that call is made, runs on `bug` issues write
no session or test output to the workflow log, in any part of the code.
Results are kept outside the folder that gets uploaded, and copied into it
only once code has classified the run. A run that stopped on a crash
uploads only its outcome, and a run that is cancelled or dies uploads
nothing.

Crash detection reads the logs the test's own servers write, so a test
written to hide a crash can still hide one. A hidden crash would at worst
lead to a fix for the wrong problem, which the checks, the reviewer session
and the people are there to catch.

If a bot PR later turns out to touch a vulnerability, the owner closes it,
the branch is deleted straight away, and `SECURITY.md` is followed. The
commits stay reachable on the fork by their hashes, as they would on any
public fork.

### Certification

A bot can't certify where code came from, so its commits carry no
sign-off. The DCO check can't make sure a person does it either. The DCO
app skips merge commits and anything authored by a bot account. It only
flags the bot's commits today because their author email doesn't match any
GitHub account, and on backport PRs a committer then approves the sign-off
by hand. If that email were ever corrected, the check would pass with
nobody having certified anything.

So certification is the owner's job, at merge time, as described under
[the pull request](#the-pull-request). Nothing on GitHub enforces it.
Rebase merges are allowed, and the squash message defaults to the PR body.
The owner may also be able to approve alone, because the PR's author is
the bot; a rule on `unstable` that asks for extra approval of unattributed
changes may prevent that (see [verify first](#verify-first)). So the weekly
report flags every merged bot PR that wasn't squashed, has no
`Signed-off-by` from its owner at the time of merge, or was approved only
by its owner.

## Limits and rollback

| Limit | Starting value |
|---|---|
| Kill switch | A repository variable in `valkey-ci-agent`. When it's off, intake leaves labels alone and upkeep does nothing. |
| Runs in progress, shared with detector runs | 2 |
| Spend per month, factory runs only | The TSC's ceiling (decision 5) |
| Runs per issue since it was last opened | 3 |
| Open bot PRs | 5 |
| Attempts at a failing test in one run | 5 |
| Attempts at a fix in one `bug` run | 2 (detector runs keep the engine's 3) |
| Automatic CI fixes per bot PR | 2 |

When nothing is happening, it costs one listing call and one query twice
an hour, and no runner is held in between. Model time is only spent inside
runs, after code has turned away everything it can. Each push starts
Valkey's PR CI once.

**Rollback.** Before it opens a PR, the bot checks two things. Has a merged
bot PR been reverted on `unstable`? Were the last three bot PRs closed
without merging, not counting one that a person's fix landed ahead of, or
whose issue turned out to be `not-a-bug`? If either is true, it posts a
comment instead, and keeps the PRs that are already open green. To lift the
hold, the operator writes down what went wrong and sets the repository
variable `FACTORY_HOLD_CLEARED` to that day's date, after which older
events no longer count.

A **revert** is a commit on `unstable` that names a bot PR or its squash
commit in any of the ways git and GitHub write it: `This reverts commit
<sha>`, `Reverts valkey-io/valkey#N`, or a `Revert "<title>"` subject.

## Measuring it

A weekly job posts the numbers as an issue in this repository.

It counts **how runs end**: a PR, a test only, nothing reproduced, no
result, or out of scope. Where a person later fixed an issue the bot
commented on, it runs two cross-checks: does the bot's test fail before
that fix and pass after it, and does the person's test pass with the bot's
patch? It also counts the 👍 and 👎 on the bot's comments, and who gave
them.

For each merged bot PR it records the **reviewer cost**: the review
comments, the committer actions it took (approvals, DCO approvals and
workflow approvals), and the owner's own comments and pushes, next to human
bug-fix PRs of a similar size merged in the same weeks. It tracks **review
comments that keep coming up**, which feed the loop in
[meeting the reviewers' standard](#meeting-the-reviewers-standard). It
counts PRs **accepted as written**, meaning merged with no change pushed by
a person, alongside those merged after a person's changes, superseded by a
person's PR, or closed. And it tracks how many PRs were **green on
arrival**, how many were **regretted** (reverted, or their issue reopened,
within 60 days), and the gaps listed under [certification](#certification).

Step 2 counts as a **success** if, over the first ten bot PRs that are
merged or closed, at least half were merged, at least three of those were
accepted as written, and the reviewer cost per merged PR was no higher than
for comparable human fixes. It's confirmed 60 days later if none was
reverted.

## The work

The overall shape of the pipeline and its containment carry over. The fix
engine doesn't, as it stands, because every part of it assumes a failed CI
job. Roughly, step 0 is large, step 1 is medium to large, and step 2 is
large.

### Step 0

**The replay.** It builds on the eval on the `ci-fix-replay-eval` branch,
but that eval needs a failed CI run with a live log for every case, scores
by how many lines overlap, never runs the person's test, and records no
cost.

The replay's cases are the 40 most recently closed `bug` issues that a
merged PR fixed and that were closed after the model's training cutoff
(after June 2026 for the current model), with none removed by hand. A case
that turns out to be out of scope counts as a failure. A few extra cases
carry hostile text planted in the issue, and are scored separately. Each
case sees only what the issue said before the fix was opened, and
`unstable` at the fix's parent commit; the history search reads nothing
later.

A case is **publishable** when three things hold. The run ends with a test
that counts and a fix that passes every check. Where the person's fix came
with a test that fails on the fix's parent commit, that test also passes
with the bot's fix. And a committer who did not write the original fix,
reading the bot's would-be PR, says they would own it. The denominator is
every case. The replay also records each run's cost and what a reviewer
would have had to read. It writes nothing to `valkey-io/valkey`.

**A new way into the engine for `bug` issues.** Today the engine refuses to
start without a CI run, a job and a job log. It fixes its verification plan
before Claude starts, its fallbacks are built around "did not pass", and
its prompt is about fixing a failed job. A `bug` run has none of that. In
the new path, phase 1 names the test, code plans its command once with no
fallbacks, and phase 2 reuses that plan. Today's loop also drops to fewer
test runs when a test run is slow; the ten-run rule doesn't.

**The two phases**, as described under
[phase 1](#phase-1-a-test-that-fails) and
[phase 2](#phase-2-a-fix-that-the-test-passes).

**The guidelines.** The bot reads the files listed under
[meeting the reviewers' standard](#meeting-the-reviewers-standard). Today
the engine reads `.github/instructions/integration-tests.instructions.md`
and doesn't read `src/unit/README.md`, so both change. It also needs the
recent fixes with their review comments, the lint and the checklist, all
read through a per-repository profile.

**The runner.** A build account and a read-only tree; AddressSanitizer and
default builds; googletest, which the bot's runner doesn't install today;
`clang-format`; and builds and test commands in a loopback-only network
namespace. Today the runner hands the whole checkout to the test account
and has no network controls at all.

### Step 1

**Label intake.** Reading the timeline event and checking the role,
refusing bots, the edit rule (from the issue's edit history and its
`renamed` events), the eligibility list, the queue, cancelling a run whose
label was removed, counting runs per issue by workflow-run name, and
keeping within the spending ceiling. The detector dispatcher only counts
active runs among the last hundred, so this needs its own record.

**Starting runs.** `ci-fix-issues.yml` and its dispatcher already start one
run per detector issue, two at a time. They extend to `bug` issues and to
labelled detector issues. A labelled detector issue that already has a
patch comment gets a fresh run, and if its logs have expired, the run is
refused and says so.

**The controls.** The kill switch, the restart rule, and the rollback hold
variable.

**Publishing to any issue.** Publication currently requires a detector
marker on the issue, and keys its result record by a CI run. Both change.

**Quiet runs.** Today the model's session goes to the workflow log as it
runs, and into the uploaded evidence. The engine logs failed test output
and the feedback it sends. A refusal's text quotes test output. Publish
prints the result to the log and the job summary. An upload with no files
fails the job, which is then counted as a failed run. For `bug` runs, each
of those changes as described under [crashes](#crashes).

**The private channel** for unexpected crashes, if decision 7 allows it.

**The publication filter, the secret scanner, and the weekly report.**

### Step 2

**Choosing per run** whether to comment or open a PR, and retiring the
`draft-pr` setting of `CI_FIX_ISSUES_PUBLISH`, which today lets detector
runs open PRs with no owner.

**The fork.** Pushing to it and opening PRs across repositories, with a
token per repository, and `agent/issue/` added to the branches the bot may
write to.

**The upkeep job.** The ready rule, the two CI fixes and their marker, the
hands-off rule and the reread before every push, reassigning the PR to its
owner, dismissing approvals, branch deletion, and the rollback check.

**CI fixes on fork heads.** The CI follow-up only works on bot PRs into
registered release branches and refuses fork heads, and `fix` falls back to
an unverified suggestion on a fork head. Both change for bot PRs, where
`fix` is authorized by role rather than by membership of the `contributors`
team.

**One concurrency group per issue, shared with its PR.** Today a detector
issue and its PR use different ones.

**Reviewer cost, acceptance and certification** in the weekly report.

Every action has to be safe to run twice: run again from the same state,
it must not push twice or post twice. The tests for each step show it.

### Verify first

Each of these gets checked on a test fork before the step that relies on
it.

| Before | What to check |
|---|---|
| Step 0 | Valkey's tests pass with loopback-only networking and a read-only tree, with the test modules and TLS certificates built beforehand. |
| Step 1 | The App's installation token can read an issue's edit history and timeline, and cancel a workflow run. |
| Step 1 | The App can create a draft security advisory, if decision 7 says it should. |
| Step 2 | Whether a PR from the bot's fork needs a committer to approve its workflow runs every time. |
| Step 2 | Whether the App's token for `valkey-io/valkey` can open a PR whose head is in a machine user's fork. |
| Step 2 | Whether committers can push to that fork's branch, and whether a fork inside `valkey-io` would be allowed and get no secrets. |
| Step 2 | Whether the App can mark a PR ready, reassign it and dismiss approvals with pull request write alone. |
| Step 2 | Whether pushing to the fork needs the `workflows` permission when the push carries `unstable`'s workflow changes. |
| Step 2 | What the `unstable` rule "require extra approval for unattributed changes" means for a PR from an App whose commits match no account. |
| Step 2 | Who GitHub records as the author of a squash commit for an App's PR. |

## Later: step 3, the review loop

Step 3 adds two commands on a bot PR. Only committers can use them, and the
comment poller picks them up within about an hour. Hands off doesn't stop
them; auto-merge does (see [hands off](#hands-off)).

```mermaid
flowchart TD
    A["A committer comments<br/>@valkeyrie-ops address"]
    B["Collect new review comments from committers<br/>apply suggestion blocks as written"]
    D["Claude answers each one:<br/>changed, answered, declined, or a conflict"]
    E["Rerun the phase 2 checks<br/>and the failing-test check if the test changed"]
    F(["One commit, a reply on each thread"])
    G(["Say what failed, push nothing"])
    A --> B --> D --> E
    E -->|"passes"| F
    E -->|"fails"| G
```

**`@valkeyrie-ops address`** applies review comments. The batch is every
inline thread whose newest committer comment is newer than the bot's last
reply in it, plus every review summary submitted since the last `address`
it handled. Comments from other people and other bots aren't instructions,
unless the owner says so in a comment of their own. `suggestion` blocks are
applied exactly as written, by code. For everything else, Claude returns a
change and, for each comment, one of four answers: changed, answered,
declined with a reason, or conflicting, when two committers asked for
incompatible things. On a conflict it changes nothing for those comments
and names both people.

Then the phase 2 checks run again. If the test changed, code runs it on
the merge base of the PR and `unstable` with only the test files applied,
and it has to fail the way it did in phase 1. If a committer told the bot
to remove the test, that is the exception in rule 1: what's left is still
built and run, and the PR body says the fail-then-pass evidence no longer
applies. The bot pushes one commit and replies on each thread. It never
resolves a thread; that's the reviewer's call.

**`@valkeyrie-ops update`** brings the PR up to date with `unstable`. The
bot never does this unprompted, since `unstable` doesn't require it. Code
runs `git merge`. Files that merge cleanly are taken exactly as git
produced them, only files with conflicts go to Claude, and code checks that
nothing else changed. The checks run again, with the bounds measured
against the merge base so that `unstable`'s own `.github/` changes don't
count against the PR. If the test now passes without the fix, someone else
fixed the bug, and the bot says so instead of pushing. It posts how the
PR's change differs before and after the update. The branch is never
rewritten, and since Valkey squashes, the merge commit never reaches
`unstable`.

The work for step 3 is parsing the two commands; pushing a merge commit,
which today's push code can't do, with the `workflows` permission it may
need; and reading and replying to review threads, which the release notes
watcher already does for one file.

One question is left for step 3 itself: should these happen without a
command, whenever a review is submitted? That would be quicker. It would
also push in the middle of reviews that arrive one comment at a time. In
the sample, most reviews involved two or more people, and about a third of
the comments were design questions rather than requests to edit.

## Alternatives we considered

**A comment command instead of a label.** A comment can carry a hint. But a
label is a queue anyone can see, it waits patiently when the bot is busy,
and removing it cancels. Its one weakness, that people with triage access
can apply labels, goes away once the bot checks who applied it.

**Letting whoever merges certify, with nobody owning the PR.** Simpler,
but it asks a person to sign for code they may only have looked at when
they pressed merge. Valkey's `CONTRIBUTING.md` says the person who submits
code owns its quality.

**Branches in `valkey-io/valkey`, the way backports work.** Simpler again,
at the cost of CI running bot-written code with secrets before anyone has
reviewed it.

**Holding back every crash.** Stricter than the project is with itself, and
it would throw away many of the bugs the bot could actually fix.

**Saying in public that a run stopped on a crash.** It would let the owner
know without a private channel, and it would tell everyone else that the
issue probably hides a crash.

**Running `bug` runs in a private repository.** That would hide even an
unexpected crash, but it means a second deployment to keep in step with
this one.

**Reading the Copilot instruction files as well.** More guidance, at the
risk of following a stale copy instead of the source.

**Putting `run-extra-tests` on every bot PR.** Wider coverage, but Daily's
flaky tests would keep the PRs red. A committer can still add the label.

**Closing idle bot PRs automatically.** With hundreds of human PRs waiting
weeks for review, this would mostly close good PRs that nobody had got to
yet.

**Rebasing and force-pushing to update.** A tidier history that gets
squashed away anyway, in exchange for reviewers losing their place.

**Reacting to events instead of polling.** Seconds instead of half an hour,
in exchange for a workflow and a key living in the Valkey repository.

**Counting a flaky reproduction.** One failure in forty test runs isn't an
estimate of anything. Phase 1 asks for three out of three.

**Opening PRs without a failing test.** That would accept fixes for bugs
nobody has demonstrated.

## Appendix: what I checked

All of this was read from GitHub or from this repository's source on
11 November 2026.

**`unstable`'s rules.** DCO is the only required status check. A branch
doesn't need to be up to date to merge. One approval is required, history
is linear, stale reviews aren't dismissed on push, and "require extra
approval for unattributed changes" is on.

**Merging.** `valkey-io/valkey` allows squash and rebase merges but not
merge commits, and allows auto-merge. The squash message defaults to the PR
body.

**Governance.** GOVERNANCE.md defines committers as people with write
access, and maintainers as people with full access, who make up the TSC.
On the repository, `contributors` (43 members) has triage access,
`valkey-release` write, `valkey-committers` maintain, and `core-team`
admin. `CONTRIBUTING.md` says Valkey does not accept anonymous contributors.

**DCO.** DCO shows `action_required` on the bot's open PR 5357, and
"Commit sign-off was manually approved" on its merged PR 5335.

**Valkey's CI and templates.** `codecov.yml` runs on push and on pull
requests, builds and tests the branch, then uploads with
`secrets.CODECOV_TOKEN` in the same job. `spell-check.yml`, `external.yml`
and `reply-schemas-linter.yml` also run on pull requests.
`benchmark-on-label.yml` runs on `pull_request_target` when a PR is
labelled, and builds the PR's merge commit with `secrets.EC2_ARM64_IP` on a
self-hosted runner. The crash-report template titles issues `[CRASH]` and
labels them `bug`, and the bug template applies `bug` too.
`SANITIZER=address` forces `MALLOC=libc`. `tests/integration/logging.tcl`
crashes servers on purpose. PR 5138 is a crash fixed in public, with its
trigger described and an "AI generated details" section.

**Valkey's guidelines.** The `.github/instructions/` files arrived with
"Add copilot review instructions" (#3076, January 2026), and two of the
three haven't changed since; `DEVELOPMENT_GUIDE.md` changed in October.

**The bot's own PRs.** None it has opened targets `unstable`.

**This repository's code.** Every "today" statement under
[the work](#the-work) was read from `main` at `b1b87c6f`. In addition, the
comment poller only understands `fix`, and the replay eval isn't on `main`
yet. `docs/setup.md` says the default model needs Bedrock's provider data
sharing.

**Not checked.** The sample figures, apart from today's counts. Which DCO
clause applies to AI-assisted work, and how other projects use
`Assisted-by:`. Who funds the AWS account. And everything listed under
[verify first](#verify-first).
