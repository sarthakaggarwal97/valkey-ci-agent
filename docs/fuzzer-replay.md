# Replaying merged pull requests through the fuzzer

The replay runs the fuzzer's whole pull request flow over history: for each
pull request merged into a Valkey branch in a range, oldest first, it does
what a live run would do when that pull request merged.

1. [Fuzzer proposals](fuzzer-proposals.md): Claude reads the merge's change,
   its diff against its parent, and proposes scenarios aimed at it, with
   experiments of its own when no installed one reaches the change. Only the
   ones `valkey-fuzzer validate` accepts are kept. A change that needs no
   test gets a reason instead (`reason` in `replay.json`).
2. valkey-fuzzer's PR batch (`fuzzer-pr-batch.yml`) builds the merge commit
   and its parent, with the chosen `build`, and runs those scenarios beside
   a random baseline: `random_seeds` seeds of every experiment that needs
   one build, drawn from the commit. A proposed experiment that compares a
   command sequence across the two builds reaches the parent as
   `artifacts.parent`.
3. The [fuzzer monitor](fuzzer-monitor.md) triages the batch
   (`--pr-batch-run`) and files or updates an issue on valkey-fuzzer for each
   finding that looks like a real problem. The issue names the merge it
   tested; a later merge that shows the same failure signature adds an
   occurrence to the same issue.

Only the replay is made up: the proposals, the batch and the monitor are the
ones a live run uses. One thing a live run never meets: a build older than the
rules a fuzzer oracle models. valkey-io/valkey#4749 changed how a full sync
picks its codec and transport, and the `repl-compression` oracle knows only
the new rules, so a merge before #4749 leaves the experiments it judges
(`valkey.repl-compression-full-sync`, `-partial-resync`, `-runtime-change`
and `valkey.replication-chaos`) out of its batch: the PR batch's
`exclude_experiments` keeps them out of the random baseline, and the replay
drops the proposals that use them (`excluded_proposals` in `replay.json`). The batches run in parallel; the triage runs one merge
at a time, in merge order, so a failure's issue opens on the first merge that
showed it.

## Running it

```bash
# The first three merges after 9.2.0-rc1 branched, as a trial
gh workflow run fuzzer-replay.yml --repo OWNER/valkey-ci-agent \
  --field range=122ec7e16..7220c38f9 --field first=3 --field fuzzer_ref=main

# Every merge in the range
gh workflow run fuzzer-replay.yml --repo OWNER/valkey-ci-agent --field range=122ec7e16..7220c38f9

# Chosen merges, in the order given, instead of a range
gh workflow run fuzzer-replay.yml --repo OWNER/valkey-ci-agent \
  --field merges="e50b62d0b bce52d39f 45ec67c30 d30102951 b3a135637"
```

| Input | Default | Meaning |
|---|---|---|
| `range` | | `BASE..TIP`: the merges after `BASE` up to `TIP`, on `TIP`'s first-parent history of `valkey-io/valkey`. |
| `merges` | | Instead of `range`: the merge commits to replay, 1 to 256 SHAs of 7 to 40 lowercase hex characters, separated by commas or spaces. Each is resolved in the Valkey clone and replayed against its first parent, in the order given. Give one of `range` and `merges`. |
| `first` | `0` | Replay only the first N merges (0: all). |
| `count` | `5` | Scenarios to ask Claude for per merge. |
| `random_seeds` | `2` | Random seeds of each one-build experiment per merge, 0 to 4. |
| `max_parallel` | `10` | Merges replayed at once. Each holds a runner while its batch holds another. |
| `fuzzer_ref` | `main` | The valkey-fuzzer branch whose PR batch runs. |
| `build` | `default` | The build of each merge commit and its parent: `default` (plain), `asan` or `ubsan`. |
| `repository` | `valkey-io/valkey` | The repository whose merges are replayed, `OWNER/NAME`: Valkey, or a module the PR batch builds (see [Modules](#modules)). |
| `valkey_ref` | `unstable` | For a module repository: the `valkey-io/valkey` branch, tag or full commit every merge loads into. |

valkey-fuzzer is `OWNER/valkey-fuzzer`, the repository owner's. GitHub
starts a dispatched workflow only when its file is on the default branch,
so `fuzzer-pr-batch.yml` must be on valkey-fuzzer's default branch and
`fuzzer-replay.yml` on this repository's.

## Modules

With `repository` set to a module the PR batch builds (`valkey-io/valkey-json`,
or a fork of it), the replay replays the module's merges the same way, on one
Valkey build:

- `plan` clones the module instead of Valkey and lists its merges as it lists
  Valkey's. It resolves `valkey_ref` to a commit once, so every merge of the
  replay loads into the same Valkey build, and records it on each merge as
  `valkey_sha` (in `replay.json` too).
- Each merge's proposals are a module change's (see
  [fuzzer proposals](fuzzer-proposals.md#modules)): its scenarios load the
  module's two builds through `artifacts.module` and `artifacts.module_parent`.
- Its PR batch builds that Valkey commit, and the module at the merge
  (`module_sha`) and at its parent (`module_parent_sha`), against it; there is
  no `valkey_parent_sha`. The module is sent even when the proposal failed.
- The #4749 exclusion is for Valkey merges: a module's merge leaves no
  experiment out.

The random baseline still draws Valkey's own one-build experiments, which do
not load the module and so run the same Valkey build at every merge; a module
replay usually wants `random_seeds=0`. A Valkey merge whose proposals name
one of Valkey's `tests/modules` (`module_file`) sends it the same way, beside
the parent build.

```bash
# Every valkey-json merge in a range, loaded into Valkey unstable as it is now
gh workflow run fuzzer-replay.yml --repo OWNER/valkey-ci-agent \
  --field repository=valkey-io/valkey-json --field range=BASE..unstable --field random_seeds=0
```

## Each merge's files

The `replay-<index>` artifact holds `replay.json` (the merge, its pull
request, the build, how many scenarios and experiments were proposed and
dropped, Claude's `reason` when the change needs no test, and the PR
batch's run), `change.diff` (the merge's diff, which the triage hands the
monitor as `--change-diff`, so it can judge whether the change accounts for
a difference between the two builds), and `proposals/` (the proposals as
[fuzzer proposals](fuzzer-proposals.md) write them).
`replay-triage-<chunk>` holds the monitor's result for each
merge it triaged, with `change_runs`: the runs that reached the change, did
not (`unreached-change`: a marked command replied the same on both builds),
showed an unexplained difference, or failed without a vetted cause. The batch folders are the PR batch runs' own
`fuzzer-batch-<run id>` artifacts on valkey-fuzzer.

## Tokens

Each job asks for the narrowest token it needs: the replay can start
workflows on valkey-fuzzer and read it, the triage can read its Actions and
write its issues. With the App's secrets (`VALKEYRIE_BOT_APP_ID`,
`VALKEYRIE_BOT_PRIVATE_KEY`) the tokens are App tokens; a fork without them
rehearses with `REHEARSAL_PAT`, and the job notes the scope the App would
have been given. Claude runs on Bedrock through `AWS_ROLE_ARN`.

## Where the code lives

`scripts/fuzzer/replay.py` lists the merges (`plan`, with `--repo-dir` a
clone of `--repository`) and replays one (`run`). The workflow is `.github/workflows/fuzzer-replay.yml`.
