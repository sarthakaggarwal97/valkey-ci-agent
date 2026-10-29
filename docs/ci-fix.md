# Fixing CI failures

The CI fix bot takes one failed CI job, works out why it failed, and proposes
a fix. It treats its own fixes with suspicion. For a fix it may publish, code
first reproduces the failure, reruns that reproduction with the fix applied,
and asks a second, independent Claude session to review it. Contributor-PR
suggestions are not executed by the bot; they are reviewed. Even then the fix
is only a proposal. A model-authored patch arrives in a comment for a
maintainer to apply. Only an exact fix already merged on the trusted default
branch may be ported as a commit on a bot-owned pull request. The bot never
merges.

## Asking for a fix

On any `valkey-io/valkey` pull request, members of `valkey-io/contributors`
can ask for a fix by starting a comment with:

```text
@valkeyrie-ops fix
```

You can point it at a particular run or job, and add a hint in your own
words:

```text
@valkeyrie-ops fix https://github.com/valkey-io/valkey/actions/runs/123/job/456 the timeout looks too tight on slow runners
```

Without a link, the bot picks the failure on the PR's latest commit that it is
most likely to be able to fix. It prefers ordinary test jobs, then lint and
build jobs for a contributor-only suggestion. For a branch it may update, it
tries sanitizer and valgrind test jobs before lint or build-only jobs, because
only a focused test failure can pass the code-owned reproduction gate. A
linked job is still honored exactly. `@valkeyrie-bot fix` works as well.

The bot looks for new requests every hour. When it picks yours up, it reacts
to your comment, and it replies on the PR when it is done.

Model-authored patches are never pushed, including on the bot's own PRs:
normal target-repository CI would execute the patch before a person reviewed
it. The bot posts those fixes as patches in comments. On a bot-owned PR it may
instead push an exact commit that code proved is already merged on the trusted
default branch.

## Other ways a fix starts

The bot also starts without being asked in two situations.

When one of the bot's own backport PRs fails CI, the backport workflow's
hourly CI follow-up hands one failed job at a time to this bot. That is
described in [Backports](backport.md#when-a-backport-prs-own-ci-fails).

When the [Test Failure Detector](test-failure-detector.md) files an issue for
a test that failed in Daily CI, the bot can try to fix that too. A maintainer
can start this for one issue by hand, and an hourly dispatcher can start it
for every eligible issue once it is switched on. See
[Fixing detector issues](#fixing-detector-issues).

## What the bot does with a failure

**It pins down exactly what failed.** The bot records the run, the attempt,
the job and the commit that failed. For a PR, the failure has to be on the
PR's latest commit, because a fix for an older commit would be fixing code
that is no longer there.

**It recreates the job where it can.** Supported Ubuntu and macOS host jobs
run on the same runner with their own build flags and test command. Trusted
bot code installs a fixed dependency set, and the target workflow's remaining
setup runs without credentials as a disposable account. Many failures only
happen on one platform or with one build configuration, so this matters.
Containers and setup outside that code-owned profile run on a plain Ubuntu
runner with a default build instead.
[How the failed job is recreated](#how-the-failed-job-is-recreated) has the
details.

**It looks for a fix that already exists.** Before writing anything, the bot
searches the git history for merged commits that mention the failing test,
change its file or a source file the error points at, or reference its issue.
On a release branch, the answer is often a fix that already landed on
`unstable` and was never backported, and porting it is better than writing a
new one. On `unstable`, a matching commit newer than the failure suggests the
problem may already be fixed.

**It tries to reproduce the failure.** For Tcl tests and C unit tests, the
bot builds the code the way the job did and runs only the failing test. It
runs it once, then in a loop of 20, then in the same loop with every CPU core
kept busy, because a lot of flaky tests only fail under load. Code admits an
executable fix only when that focused command shows the named test failing.
It deliberately does not infer reproduction from a whole job, a generic
nonzero exit, or text that merely resembles the original log.

Before any executable fix, code must establish that baseline itself. If no
focused command can be planned, the command cannot run, every probe passes,
or the probes end without observing the named failure, the bot records the
evidence and stops before launching the edit agent. A broken build, a missing
tool, or a crash in the test harness does not count. The baseline command must
also leave the tracked and non-ignored checkout state exactly as it found it;
if it changes that state, the bot restores the original state and stops before
any edit. The job's setup must start from that same clean tracked state, so a
setup-generated source change can never be mistaken for part of the agent's
patch. Ignored build output is deliberately allowed.

**Claude diagnoses and edits.** Claude gets the job's log, whatever the
history search found, the result of the reproduction, and Valkey's testing
conventions: the bot's own guide in `scripts/ci_fix/guides/valkey-io__valkey.md`
plus the repository's `tests/README.md` and contributor docs. It then does one
of three things: ports an existing fix (preferred when one fits), writes a
small patch, or declines. Declining is a legitimate answer, since some
failures need a person. Claude works in a scratch copy of the code. It can
edit files there but cannot commit or push.

**Code checks what Claude handed back.** The bot rejects anything that doesn't
add up: a commit Claude made itself, files changed that Claude didn't
declare, a diagnosis with parts missing, a port that isn't a real merged
commit or is already on the branch, or a suggested patch that is too large.

## How the fix is tested

A fix for a bot PR or a detector issue has to pass a feedback loop before it
is published. The loop runs at most three rounds and has an hour in total,
of which reproducing the failure may take up to twenty minutes.

In each round, a port is first cherry-picked into the code. If it conflicts,
Claude can adapt it into a patch instead. Then the same code-owned baseline
is run again with the fix applied. Focused tests run in a loop, and under
load if that is how they failed before: 10 times if the test failed on every
plain run, 40 times if it only failed sometimes or Claude judged it flaky,
and 4 times under valgrind.

Finally a second Claude session, which can read the code but not change it,
reviews the fix with the same conventions and test results in front of it,
and has to approve. The reviewer derives its judgment from the log, patch,
code and code-owned reproduction rather than trusting the edit agent's
diagnosis. If the tests fail or the reviewer objects, the output or the
reviewer's reasoning goes back to Claude, which revises its fix for the next
round. If the code-owned build or reproduction command changes any
tracked file while the fix is being tested, the fix that was tested is no
longer the fix in the tree, so that also goes back for another round.

A fix that doesn't pass within three rounds is declined. Exact-job and
plain-Ubuntu fallback paths use the same admission rule: only a failure that
code reproduced may reach the edit agent. Missing, unrunnable, incomplete and
passing reproduction attempts end with a clean refusal. If the bot runs out
of time or crashes after a failure was reproduced, that isn't treated as
declining: the CI follow-up and detector issues get one more attempt at the
same failure.

Patches suggested on contributor PRs are reviewed but not run, so they are
the only targets that do not require an executable reproduction. The bot does
not accept or publish model-authored commands, summaries, diagnoses or review
prose as maintainer guidance. Comments contain the patch plus code-owned
status and verification evidence; automatically pushed commits are exact
ports of already-merged upstream changes.

## Fixing detector issues

When the Test Failure Detector files an issue, the bot can attempt a fix. You
can start it for one issue with the **CI Fix Issues** workflow. An hourly
dispatcher also starts it for every open detector issue that is eligible.
Setting the repository variable `CI_FIX_ISSUES_AUTO_DISPATCH` to `false`
turns the dispatcher off.

An issue is eligible when it is open, unassigned and has no open PR linked to
it, and when the bot hasn't already posted a fix, declined, or failed twice
for the failure it currently records. A newer failure makes the issue
eligible again. At most two of these fixes run at once, which
`CI_FIX_ISSUES_MAX_RUNS` can change. Right before starting, the bot checks
again that it hasn't already reported on that failure, so a dispatch that is
retried can never post a second patch.

The fix is prepared on the current tip of the branch that Daily CI tested,
normally `unstable`, using the original log and commit as evidence. Only the
issue's own test is reproduced and checked. Other tests that failed in the
same job have issues of their own.

By default the bot posts the tested patch on the issue together with what code
checked. Nothing is pushed and no PR is opened. A maintainer can apply the
patch or ignore it, and the issue isn't tried again until the test fails again.

If the candidate is an exact fix already merged on the trusted default
branch, `CI_FIX_ISSUES_PUBLISH=draft-pr` lets the bot port that commit to a
branch named `agent/ci-fix/issue-<issue>-<run>` and open a draft PR that says
`Fixes #<issue>`. Model-authored patches remain issue comments regardless of
this setting. One test file often has several open issues, so if a candidate
touches another issue's test, the bot verifies that test separately before
claiming the candidate fixes it.

## How the failed job is recreated

To run the fix in the environment that failed, the bot reads the workflow
file that ran the failed job. That is `daily.yml` for a nightly failure, or
the PR's own workflow otherwise, read at the commit the fix will be made on.
It finds the job whose name matches the one that failed, including the
specific matrix entry, and evaluates the `${{ }}` expressions it needs the way
GitHub would have for that run.

The fix job checks out the code and, when declared, its pinned
`libbacktrace` repository. It accepts only Ubuntu and macOS host setup covered
by a code-owned profile. On Ubuntu, trusted bot code installs a fixed superset
of Valkey build and test packages with fixed `apt-get` arguments. Canonical
workflow lines that install only those packages are then omitted. The one
pinned `libbacktrace` install is rewritten from a system install to a private
prefix inside that checkout. The extracted `make` step is never replayed as
setup; the reproduction engine owns every main Valkey build. Remaining
admitted setup runs as a disposable account with a private home, temporary
directory and GitHub command files, and without the workflow's credentials or
OIDC request variables. For an admitted host job, a setup step written after
`make` is moved earlier only when it is a canonical Ubuntu package action that
trusted bootstrap completely replaces; anything else falls back rather than
assuming it is independent of build output.

Where the job would have run its tests, the bot prepares the fix instead,
rebuilding with the job's own `make` command and testing with its own
`runtest` line. The exact-SHA checkout's `.git` directory remains runner-owned
and its inode is verified across target setup, so the disposable setup account
cannot replace or modify Git metadata. Before reproduction, the engine also
rejects any tracked setup change. Every untrusted phase uses that one checkout
below the operating system's physical `/tmp` directory. The runner workspace
remains trusted action state and is never handed to a disposable account.

The host profile needs one `make` step, one focused `runtest` or C-unit-test
command, and setup made only of scripts and checkouts. A container, VM,
secret, unsupported action or expression, unknown privileged command, or
noncanonical package install sends the fix to the bot's own Ubuntu runner.
The same code-owned reproduction gate applies there, so fallback never means
that an unreproduced failure reaches the edit agent.

## Keeping the bot contained

No target-repository token is passed to target setup, Claude, build or test
processes. Separate jobs before and after the fix hold short-lived App tokens
for reading the failure and publishing the result.

In exact-job mode, target setup first runs as a disposable `ci-fix-setup`
account. It can own the checkout but cannot read the plan, trusted agent
checkout, real runner temp, GitHub command files, OIDC request variables or
later AWS credentials. Every process of that account must stop before the tree
is reclaimed; otherwise the job fails closed and the trusted agent is not
handed back. Its numeric identity stays reserved and quiescent, without
ownership of the checkout, until final cleanup. A protected marker binds each
fixed account name to the exact numeric user and group created by this run, so
fallback cleanup never kills or deletes an unrelated pre-existing identity.

Build and test commands run as `ci-fix-command`, while Claude runs as
`ci-fix-agent`. Neither can read the workflow's environment. Claude is started
with only the AWS credentials it needs for the model, and the command account
gets no credentials at all. Local mode uses those two accounts; exact-job mode
also has the setup account. Every identity present must be distinct, and every
process under each identity must stop before the checkout is reclaimed or the
accounts are removed. Valkey's build and test dependencies are installed
before any credentials exist.

Each untrusted account owns the code only while it works, so the bot is careful when it takes
the code back. Fresh, runner-owned `.git` metadata stays non-writable to the
account, and replacement is detected before the checkout is trusted again.
Before testing a fix, the bot deletes ignored files that the job's own setup
didn't create, so a prebuilt binary can't stand in for the real build.

The bot never pushes a model-authored patch, under `.github/` or anywhere
else. A backport branch may receive an exact `.github/` commit that code
proved is already merged on the trusted default branch; only that port path
gets a token with `workflows: write`. An unproven orphan issue branch is never
turned into a pull request. The bot's comment lists everything it checked,
including the history search and the reproduction.

## Running it by hand

To ask for a fix on a PR, dispatch **CI Fix Bot**. You are checked the same
way a commenter would be, so you need to be a member of
`valkey-io/contributors`, and `repo` must be `valkey-io/valkey`. Leave
`run_url` empty to let the bot choose the failure.

```bash
gh workflow run ci-fix.yml --repo valkey-io/valkey-ci-agent \
  --field repo=valkey-io/valkey --field pr=1234 \
  --field run_url=https://github.com/valkey-io/valkey/actions/runs/123456789/job/987654321
```

To pick up fix comments now instead of waiting for the hourly check:

```bash
gh workflow run ci-fix-comment-poll.yml --repo valkey-io/valkey-ci-agent \
  --field lookback_minutes=180
```

To try to fix one detector issue:

```bash
gh workflow run ci-fix-issues.yml --repo valkey-io/valkey-ci-agent --field issue=1234
```

To see which issues the dispatcher would start fixes for, without starting
any:

```bash
gh workflow run ci-fix-issue-dispatch.yml --repo valkey-io/valkey-ci-agent --field dry_run=true
```

## Configuration

The bot uses the shared secrets described in [setup.md](setup.md). Its
behaviour can be adjusted with these repository variables:

| Variable | Default | What it does |
|---|---|---|
| `CI_FIX_ISSUES_AUTO_DISPATCH` | unset | Set to `false` to stop the hourly dispatcher from starting fixes for detector issues. |
| `CI_FIX_ISSUES_MAX_RUNS` | `2` | How many detector issue fixes can run at once. |
| `CI_FIX_ISSUES_PUBLISH` | `comment` | For an already-merged upstream port, `comment` proposes it on the issue and `draft-pr` opens a draft PR. Model-authored patches always remain comments. |
| `CI_AGENT_CLAUDE_MODEL` | unset | Use a different model, for example `opus`. |

`CI_FIX_AGENT_USER` and `CI_FIX_COMMAND_USER` are internal isolation settings;
trusted `job.sh` sets them to the distinct model and command accounts before
the engine starts. Which jobs the bot ignores, and whether it follows up on
backport PRs, is set per repository in [`repos.yml`](registry.md).

Comment requests are currently limited to `valkey-io/valkey`. Most of the
engine doesn't care which repository it works on, but recreating a job and
reproducing a failure follow Valkey's `make` and `runtest` conventions, and
another repository would need its own tokens and triggers.

## Where the code lives

Every fix runs as the same three stages in `ci-fix-run.yml`, driven by
`scripts/ci_fix/run.py`: a plan job that pins down the failure and works out
how to recreate it, either `fix-in-job` or `fix-local`, and a publish job.
`fix-in-job` uses a supported host runner profile; `fix-local` is the
fail-closed fallback for containers and every setup the profile does not
recognize. The jobs pass their state to each other as JSON (`state.py`).

Each way of starting a fix has a small front door that checks the request and
turns it into the same `FixTask`: `main.py` for PR requests,
`comment_poll.py` for picking up comments, `issues.py` and
`issue_dispatch.py` for detector issues, and `scripts/backport/ci_followup.py`
for backport PRs. `gate.py` parses commands and checks that the requester is
allowed. `github_runs.py` and `issue_state.py` read the state of runs and
issues.

`ci_job.py` reads the workflow file and writes the script that replays a
job's setup, and `job.sh` runs the fix inside that job. `engine.py` runs the
fix itself. It calls `prior_fixes.py` to search the history, `reproduce.py`
to reproduce the failure, and `agent.py` for the Claude sessions that fix and
review. `publish.py` and `push.py` check the destination again and publish.
`comment.py` writes the bot's reply. `policy.py` lists what each kind of
target allows, and `models.py` has the shared data types.

The limits mentioned on this page are constants in `engine.py`:
`PREPARE_BUDGET_S` (an hour), `REPRO_BUDGET_S` (twenty minutes),
`MAX_ROUNDS` (three), and the deterministic, flaky, and valgrind loop counts
owned by each `ReproPlan` in `reproduce.py`.
