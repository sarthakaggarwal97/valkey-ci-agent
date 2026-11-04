# Replaying merged pull requests through the fuzzer

The replay runs the fuzzer's whole pull request flow over history: for each
pull request merged into a Valkey branch in a range, oldest first, it does
what a live run would do when that pull request merged.

1. [Fuzzer proposals](fuzzer-proposals.md): Claude reads the merge's change,
   its diff against its parent, and proposes scenarios aimed at it. Only the
   ones `valkey-fuzzer validate` accepts are kept.
2. valkey-fuzzer's PR batch (`fuzzer-pr-batch.yml`) builds the merge commit
   and runs those scenarios beside a random baseline: `random_seeds` seeds of
   every experiment that needs one build, drawn from the commit.
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
```

| Input | Default | Meaning |
|---|---|---|
| `range` | | `BASE..TIP`: the merges after `BASE` up to `TIP`, on `TIP`'s first-parent history of `valkey-io/valkey`. |
| `first` | `0` | Replay only the first N merges (0: all). |
| `count` | `5` | Scenarios to ask Claude for per merge. |
| `random_seeds` | `2` | Random seeds of each one-build experiment per merge, 0 to 4. |
| `max_parallel` | `10` | Merges replayed at once. Each holds a runner while its batch holds another. |
| `fuzzer_ref` | `main` | The valkey-fuzzer branch whose PR batch runs. |

valkey-fuzzer is `OWNER/valkey-fuzzer`, the repository owner's. GitHub
starts a dispatched workflow only when its file is on the default branch,
so `fuzzer-pr-batch.yml` must be on valkey-fuzzer's default branch and
`fuzzer-replay.yml` on this repository's.

## Each merge's files

The `replay-<index>` artifact holds `replay.json` (the merge, its pull
request, how many scenarios were proposed and dropped, and the PR batch's
run) and `proposals/` (the proposals as [fuzzer proposals](fuzzer-proposals.md)
write them). `replay-triage-<chunk>` holds the monitor's result for each
merge it triaged. The batch folders are the PR batch runs' own
`fuzzer-batch-<run id>` artifacts on valkey-fuzzer.

## Tokens

Each job asks for the narrowest token it needs: the replay can start
workflows on valkey-fuzzer and read it, the triage can read its Actions and
write its issues. With the App's secrets (`VALKEYRIE_BOT_APP_ID`,
`VALKEYRIE_BOT_PRIVATE_KEY`) the tokens are App tokens; a fork without them
rehearses with `REHEARSAL_PAT`, and the job notes the scope the App would
have been given. Claude runs on Bedrock through `AWS_ROLE_ARN`.

## Where the code lives

`scripts/fuzzer/replay.py` lists the merges (`plan`) and replays one
(`run`). The workflow is `.github/workflows/fuzzer-replay.yml`.
