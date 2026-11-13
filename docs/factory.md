# Fixing labelled issues

The factory turns an issue a committer has labelled into a tested pull
request. A committer labels an issue `needs-agent-fix`. The bot writes a test
that demonstrates the bug, or encodes the agreed behaviour of a small feature,
then writes the change that makes that test pass, checks it the way a reviewer
would, and opens a draft pull request. Committers review and merge it like any
other pull request. The bot never merges.

It is built on the [CI fix bot](ci-fix.md): the same engine, the same isolated
account, the same publishing path and the same three-job run. What is new is
the way in, from an issue instead of a failed CI job, and the evidence it
makes for itself first.

## Asking for a fix

Apply the `needs-agent-fix` label to an open issue of `valkey-io/valkey`.
Anyone with triage access can apply a label, so the bot checks who applied it:
it acts only on a label applied by someone with write access to the
repository, and takes the label off again, saying why, when it was not. It
also stays away from an issue that is assigned, or that an open pull request
already says it fixes, and takes the label off those too. To ask again after
the bot has answered, remove the label and apply it again: each labelling is
one request.

An issue labelled `bug` is a bug report; one labelled `enhancement` without
`bug` is a feature request. An issue whose title or text shows a crash is a
crash report, and a test that crashes the server is the failure the bot
expects there. The committer decides when labelling whether a crash is a
potential security issue, and follows `SECURITY.md` instead if so.

The dispatcher looks for labelled issues every hour and starts one run per
issue, newest first, up to `FACTORY_MAX_RUNS` at once. A run takes a few
hours. When it is done, the bot comments on the issue with what it did.

## What the bot does with an issue

**It pins the issue.** The plan job reads the issue as it was when labelled:
its title and text, and the comments committers had written by then. Nothing
written after the labelling, and nothing written by anyone without write
access, reaches the model. Anyone can write an issue, and an issue can try to
steer the model; the bot treats everything in it as data, and the checks
below do not depend on what the model concluded.

**It writes the evidence first.** One Claude session writes a test from the
issue, in the main Tcl suite or as a C unit test, in the existing file for the
area, following Valkey's own test guidelines. It changes nothing but test
files. Code then builds the current `unstable` tip and runs the test three
times. The test counts only when it fails every time, on its own assertion
(`[err]`) rather than on a missing command or a broken build. A test that
passes proves nothing and goes back to the session with the output, up to
five times. For a feature the session first writes the agreed behaviour as a
short list of acceptance criteria, from the issue and the committers' comments
alone; if it cannot without guessing, the run stops and the bot posts the
questions a committer has to answer. Otherwise the tests encode the criteria
and fail because the feature does not exist yet.

A crash nobody expected stops the run. The bot posts only that it stopped and
why on the issue; the test and the crash stay in the run's artifact in this
repository, for a committer to look at first.

**It freezes the test and writes the fix.** Code commits the test in the
checkout and hands the engine a failure whose log is the issue and the test's
output, whose failing test is the one it just wrote, and whose test files are
frozen: a change that touches them is put back and goes back to the session.
From here the fix is prepared as every CI fix is: a session diagnoses and
edits, code builds the change and runs the test, once and then forty times,
and a separate read-only session reviews it, up to three rounds. The policy
differs from a CI fix's in one respect: a CI fix looks for the fault in the
test first, because flaky tests usually are at fault; here the test is the
specification, and the fix lives in `src/`. The session is given the bot's
guide for a fix from an issue (`scripts/factory/guides/valkey-io__valkey.md`),
followed by Valkey's own `AGENTS.md`, `DEVELOPMENT_GUIDE.md` and test
READMEs, and what the engine gives every fix: merged commits that mention the
issue or change the files it names, and the test file's history.

**It checks what a reviewer would otherwise find.** Before the review, code
checks the change further, in the order of their cost. The change may not
touch `.github/` or `deps/`, may not add a command (a new command is a
decision for the TSC), and may not add and remove more than 400 lines. Every
test file the test or the fix changed has to pass in full. The C files the fix
changed have to satisfy `clang-format-18`, as Valkey's CI requires, and a
changed command definition has to leave `make commands.def` with nothing to
regenerate. Finally the test runs once with the change built under
AddressSanitizer. Anything that looks like a credential in the change ends the
round. A check that fails sends the change back with the output, like a
failing test.

**A second session tries to break it.** The reviewer is told to find the case
the change still gets wrong: what the issue describes that the test does not
check, such as a restart or a replica, or an acceptance criterion the tests
miss. It has to name a specific counterexample or explain why none exists,
and it may write the counterexample as a self-contained Tcl test. Code runs
that test with the fix. A counterexample that fails sends the change back
with the reviewer's reasoning; one that passes is noted in the report. The
reviewer also rejects a change larger than the cause requires, one that does
not match the surrounding code, or one that would be hard to backport.

**It publishes one patch.** The test and the fix go out together, as one
commit against the `unstable` tip the run started from. Until the TSC has
decided (see below), the fix is posted as a patch on the issue, with the
diagnosis and everything code checked, and nothing is pushed. At the
`draft-pr` step the bot pushes a branch named `agent/factory/issue-<issue>-<labelling>`
and opens a draft pull request that says `Fixes #<issue>`. Its description
starts with a summary suitable for the squash commit, and puts the root cause,
the fix, the checks and the model's own notes under an "AI generated details"
heading. Its commit carries no DCO sign-off: whoever takes the change over
signs it off. The branch goes to the fork named by `FACTORY_FORK` when one is
set, so that the pull request has an outside contributor's permissions, and to
`valkey-io/valkey` otherwise.

The bot's text can never notify anyone, link out, or close another issue:
every mention, link and closing keyword the model wrote is rendered as plain
text in the comment or the pull request.

**A run that stops early still comments.** When no test could be written, the
comment says what the bot found and what is missing. When a test was written
and no change held up to the checks and the review, the comment carries the
failing test, so that whoever picks the issue up starts from it.

## Rollout

Each step has to earn the next. The step is the repository variable
`FACTORY_STEP`, and until it is set the bot does nothing on its own.

| Step | `FACTORY_STEP` | What runs | Moves on when |
|---|---|---|---|
| 1. Replay | unset | **Factory Replay**, by hand, over issues whose fix is known. Nothing is written to Valkey. | A committer who did not write the original fix would merge the bot's change in at least 1 case in 5 |
| 2. Comments | `comment` | The label is live. The bot posts its test and change as a comment on the issue. | Of the first 10 comments, at least half are confirmed useful by a later human fix or by a committer other than the one who labelled the issue |
| 3. Pull requests | `draft-pr` | Draft pull requests, at most `FACTORY_MAX_OPEN_PRS` open at once. | Of the first 10, at least half merge, with no more review comments than comparable human fixes, and none reverted within 60 days |

At step 3 the dispatcher stops itself when a bot pull request is reverted, or
when three in a row are closed without merging, until someone has looked at
why; what stopped it is in the dispatcher's log. Setting `FACTORY_STEP` back
to `off` stops new runs; a run already started finishes as a comment.

### Replaying known fixes

The cases are in `scripts/factory/replay_cases.yml`: one closed issue each,
and the merged pull request that fixed it. A replay pins the issue as it was
when that pull request was opened, prepares the fix on the commit the pull
request merged onto, so that the fix that landed is not there to find, and
then applies the bot's test to the known fix. A test that fails without the
fix and passes with it caught the bug the person fixed; the score says so,
and how many of the bot's own lines the known fix shares, so a person reads
the changes that differ. One run is one case:

```bash
gh workflow run factory-replay.yml --repo valkey-io/valkey-ci-agent --field case=xinfo-groups-lag
```

The score is the run's `factory-replay-score` artifact and its summary.

## Running it by hand

To fix one labelled issue now:

```bash
gh workflow run factory.yml --repo valkey-io/valkey-ci-agent --field issue=1234
```

To see which issues the dispatcher would start runs for, without starting
any:

```bash
gh workflow run factory-dispatch.yml --repo valkey-io/valkey-ci-agent --field dry_run=true
```

## Configuration

The bot uses the shared secrets described in [setup.md](setup.md). These
repository variables adjust it:

| Variable | Default | What it does |
|---|---|---|
| `FACTORY_STEP` | unset | `off`, `comment` or `draft-pr`: the rollout step. Unset or `off`, the dispatcher starts nothing. |
| `FACTORY_MAX_RUNS` | `1` | How many factory runs can be active at once. |
| `FACTORY_MAX_OPEN_PRS` | `5` | At `draft-pr`, how many of the bot's pull requests may be open before it stops. |
| `FACTORY_FORK` | unset | The fork the branch is pushed to, such as `valkeyrie-bot/valkey`. The App must be installed there. Unset pushes to `valkey-io/valkey`. |
| `CI_AGENT_CLAUDE_MODEL` | unset | Use a different model, for example `opus`. |

## Where the code lives

A run is `factory.yml`, which calls `ci-fix-run.yml` with the `factory` front
door; its fix always runs on the bot's own runner (`fix-local`), since there
is no CI job to recreate. `scripts/factory/main.py` is the front door: it
checks the label, pins the issue, plans the fix and reports the result.
`prepare.py` runs the two phases in one checkout: `evidence.py` writes and
checks the test, then the CI fix engine (`scripts/ci_fix/engine.py`) prepares
the fix with `review.py`'s adversarial review and `checks.py`'s further
checks. `dispatch.py` is the hourly dispatcher, and `replay.py` the replay
and its score. `guides/` holds the bot's policy for a fix from an issue, per
repository. The engine's own hooks for this are `FixTask.frozen_paths` and
`prepare_fix`'s `check_func`, `budget_s` and `guide`.
