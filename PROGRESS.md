# Fuzzer walk

Updated 2026-11-07T07:03+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 566 | 1941 | #3619 (2026-05-05) | 275 | 80.9 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 372 | 297 | #607 (2026-01-29) | 176 | 53.1 |

## Last runs

### valkey-io/valkey

[2026-11-07T06:28:34+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42340727393): 20 merge(s) tested, 6 without a code change.

- [valkey-io/valkey#5311](https://github.com/valkey-io/valkey/pull/5311) ([tests](tests/valkey/5311-b8d08dfe2/)): Fix uninitialized variable use in clusterManagerMigrateKeysInSlot (#5311): declined: The change fixes an uninitialized-variable read in valkey-cli's clusterManagerMigrateKeysInSlot (src/valkey-cli.c): in the parent build, 'dots', 'reply' and 'migrate_reply' were declared after the cliSelect() call, and the 'if (cliSelect(...) == VALKEY_ERR) goto next;' branch jumps over th
- [valkey-io/valkey#4470](https://github.com/valkey-io/valkey/pull/4470) ([tests](tests/valkey/4470-aa7545348/)): Validate slot import ranges in valkey-check-rdb when checking slot RDB (#4470): 1 of 1 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3374](https://github.com/valkey-io/valkey/pull/3374): Fix module loading failure for redis module (#3374): error: 2 of 2 run(s) could not start: the run could not start: environment acquisition failed
- [valkey-io/valkey#3683](https://github.com/valkey-io/valkey/pull/3683) ([tests](tests/valkey/3683-ad3b33e3b/)): cmake: Fix missing release_header dependency for cli, benchmark, and gtest (#3683): declined: The change only adds CMake add_dependencies(... release_header) lines for valkey-cli, valkey-benchmark, and the gtest library, fixing a parallel-build race where the generated release.h header might not exist yet. It is purely a build-system fix with no change to any binary's runtime behav
- [valkey-io/valkey#3719](https://github.com/valkey-io/valkey/pull/3719) ([tests](tests/valkey/3719-0321a69e6/)): Fix deferred freeClient clobbering replication state after replicaof (#3719): 1 of 8 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3697](https://github.com/valkey-io/valkey/pull/3697) ([tests](tests/valkey/3697-fdf13ca0d/)): Update deps/libvalkey to version 0.5.0 (#3697): 0 of 4 test(s) reached the change
- [valkey-io/valkey#3689](https://github.com/valkey-io/valkey/pull/3689) ([tests](tests/valkey/3689-5b167f455/)): Add check to disallow `CLUSTER SETSLOT` to target self (#3689): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3667](https://github.com/valkey-io/valkey/pull/3667) ([tests](tests/valkey/3667-4344e5b6f/)): Minor cleanup and fix typo in bzmpop get_keys_function (#3667): declined: The change only renames BZMPOP's get_keys_function from blmpopGetKeys to bzmpopGetKeys. Both functions are identical wrappers calling genericGetKeys(0, 2, 3, 1, argv, argc, result) in src/db.c, and BZMPOP/BLMPOP share the same argument layout, so key extraction (COMMAND GETKEYS, cluster sl
- [valkey-io/valkey#3679](https://github.com/valkey-io/valkey/pull/3679) ([tests](tests/valkey/3679-d4337d666/)): Fix CLUSTERSCAN fingerprint to use configurable_hash_seed (#3679): 3 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3544](https://github.com/valkey-io/valkey/pull/3544) ([tests](tests/valkey/3544-fdd903930/)): IO-Threads redesign cleanup work (#3544): 0 of 7 test(s) reached the change, coverage 82%
- [valkey-io/valkey#2993](https://github.com/valkey-io/valkey/pull/2993) ([tests](tests/valkey/2993-ca9dee353/)): Add optional REPLACE argument to MOVE (#2993): 4 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3606](https://github.com/valkey-io/valkey/pull/3606) ([tests](tests/valkey/3606-89b7baa59/)): Speed up cluster startup by 10 seconds (#3606): 1 of 3 test(s) reached the change, coverage 100%; [check, introduced by #3606](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/89)
- [valkey-io/valkey#3613](https://github.com/valkey-io/valkey/pull/3613): to retry, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#3643](https://github.com/valkey-io/valkey/pull/3643) ([tests](tests/valkey/3643-96a6bc5d2/)): Fix the memory leak in valkey-benchmark (#3643): 0 of 1 test(s) reached the change, coverage 50%
- [valkey-io/valkey#3391](https://github.com/valkey-io/valkey/pull/3391) ([tests](tests/valkey/3391-6c9d7fc26/)): CLUSTERSCAN range bounded scanning across contiguous slots (#3391): 4 of 6 test(s) reached the change, coverage 92%
- [valkey-io/valkey#3448](https://github.com/valkey-io/valkey/pull/3448) ([tests](tests/valkey/3448-75fee11c6/)): Fixes server crash when RDMA benchmark clients disconnect (#3448): declined: The change is confined to src/rdma.c: it fixes pending_list corruption (double-add and premature node removal) when an RDMA client disconnects. Exercising it requires a BUILD_RDMA build, RDMA hardware or the rdma_rxe soft-RoCE kernel module (root-only, unavailable in these runs), and a cli
- [valkey-io/valkey#3641](https://github.com/valkey-io/valkey/pull/3641) ([tests](tests/valkey/3641-2f1610771/)): Add null check in updateSSLPendingFlag (#3641): declined: The change adds a defensive null check in updateSSLPendingFlag (src/tls.c). It is unreachable as a behaviour change: the function's only three callers (connTLSRead, connTLSSyncRead, connTLSSyncReadLine) each invoke it immediately after SSL_read(conn->ssl, ...), so on any path where conn->s
- [valkey-io/valkey#3481](https://github.com/valkey-io/valkey/pull/3481) ([tests](tests/valkey/3481-9669b2670/)): Reduce latency spikes during rehashing via incremental page release (#3481): 0 of 5 test(s) reached the change, coverage 93%, 1 skipped (required capability 'build' is unsupported)
- [valkey-io/valkey#3578](https://github.com/valkey-io/valkey/pull/3578): to retry, 3 of 3 run(s) could not start: the run could not start: environment acquisition failed
- [valkey-io/valkey#3492](https://github.com/valkey-io/valkey/pull/3492) ([tests](tests/valkey/3492-c5959266f/)): valkey-benchmark: centralize RDMA WRITABLE kick via createFileEvent (#3492): 0 of 4 test(s) reached the change, coverage 87%

### valkey-io/valkey-bloom

[2026-11-07T07:02:23+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42348502641): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-07T07:03:42+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42348609622): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-07T05:20:38+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42324778733): 20 merge(s) tested, 5 without a code change.

- [valkey-io/valkey-search#741](https://github.com/valkey-io/valkey-search/pull/741): to retry, `deps=/opt/valkey-search-deps` failed in parent (exit 1): [138/223] Building CXX object src/commands/CMakeFiles/commands.dir/commands.cc.o FAILED: [code=1] src/commands/CMakeFiles/commands.dir/command
- [valkey-io/valkey-search#735](https://github.com/valkey-io/valkey-search/pull/735): to retry, `deps=/opt/valkey-search-deps` failed in merge (exit 1): [138/223] Building CXX object src/commands/CMakeFiles/commands.dir/commands.cc.o FAILED: [code=1] src/commands/CMakeFiles/commands.dir/commands
- [valkey-io/valkey-search#734](https://github.com/valkey-io/valkey-search/pull/734): to retry, `deps=/opt/valkey-search-deps` failed in merge (exit 1): [137/221] Building CXX object src/commands/CMakeFiles/commands.dir/commands.cc.o FAILED: [code=1] src/commands/CMakeFiles/commands.dir/commands
- [valkey-io/valkey-search#621](https://github.com/valkey-io/valkey-search/pull/621): to retry, `deps=/opt/valkey-search-deps` failed in merge (exit 1): [137/221] Building CXX object src/commands/CMakeFiles/commands.dir/commands.cc.o FAILED: [code=1] src/commands/CMakeFiles/commands.dir/commands
- [valkey-io/valkey-search#707](https://github.com/valkey-io/valkey-search/pull/707) ([tests](tests/valkey-search/707-aa8261ef0/)): Implement WEIGHT text field option, require a 1.0 value (#707): 4 of 4 test(s) reached the change; crash, predates #707, since fixed (recorded)
- [valkey-io/valkey-search#622](https://github.com/valkey-io/valkey-search/pull/622) ([tests](tests/valkey-search/622-69658d902/)): Introduce ShardedAtomic for zero-contention, high-concurrency counters (#622): declined: no proposed test validated
- [valkey-io/valkey-search#701](https://github.com/valkey-io/valkey-search/pull/701) ([tests](tests/valkey-search/701-300fe700c/)): Stem search terms regardless of stemsize - Bug fix (#701): 2 of 2 test(s) reached the change; crash, fixed by #701 (recorded); crash, fixed by #701 (recorded)
- [valkey-io/valkey-search#699](https://github.com/valkey-io/valkey-search/pull/699) ([tests](tests/valkey-search/699-014309dd7/)): Fix error message to match parsing logic (#699): 2 of 2 test(s) reached the change; crash, predates #699, since fixed (recorded)
- [valkey-io/valkey-search#688](https://github.com/valkey-io/valkey-search/pull/688) ([tests](tests/valkey-search/688-fe6c99b31/)): Rax Integration (#688): 0 of 4 test(s) reached the change; crash, predates #688, since fixed (recorded)
- [valkey-io/valkey-search#668](https://github.com/valkey-io/valkey-search/pull/668) ([tests](tests/valkey-search/668-e73481fd9/)): Add Memory Tracking for Index Schema Attributes (#668): 4 of 4 test(s) reached the change; crash, predates #668, since fixed (recorded)
- [valkey-io/valkey-search#679](https://github.com/valkey-io/valkey-search/pull/679) ([tests](tests/valkey-search/679-24d9b083e/)): Refactor Dockerfile to Reduce Image Layers and Size (#679): declined: The change modifies only .devcontainer/Dockerfile, merging apt install steps into fewer layers and cleaning caches to shrink the dev-container image. It is purely a build/CI environment change: no module source, command handling, indexing, replication or persistence code changes, so the lo
- [valkey-io/valkey-search#670](https://github.com/valkey-io/valkey-search/pull/670) ([tests](tests/valkey-search/670-a1b376933/)): Fix misleading error message when query string is error or not support. (#670): 2 of 2 test(s) reached the change; crash, predates #670, since fixed (recorded)
- [valkey-io/valkey-search#608](https://github.com/valkey-io/valkey-search/pull/608) ([tests](tests/valkey-search/608-d300acff7/)): Endurance failover (#608): declined: The change modifies only valkey-search's own integration test harness (testing/integration/stability_runner.py, stability_test.py, utils.py), adding failover coordination to its Python stability tests. No module source code changes, so the built module's behaviour is identical at the chang
- [valkey-io/valkey-search#669](https://github.com/valkey-io/valkey-search/pull/669) ([tests](tests/valkey-search/669-13bea40e9/)): Solving text completely in entries fetcher (#669): 4 of 4 test(s) reached the change; crash, predates #669, since fixed (recorded)
- [valkey-io/valkey-search#630](https://github.com/valkey-io/valkey-search/pull/630) ([tests](tests/valkey-search/630-06ea5f2a1/)): Add fulltext metrics to info (#630): declined: no proposed test validated
- [valkey-io/valkey-search#677](https://github.com/valkey-io/valkey-search/pull/677) ([tests](tests/valkey-search/677-1772b56d5/)): Revert "Rax Integration (#605)" (#677): 0 of 4 test(s) reached the change; crash, predates #677, since fixed (recorded)
- [valkey-io/valkey-search#605](https://github.com/valkey-io/valkey-search/pull/605) ([tests](tests/valkey-search/605-ab8da9c75/)): Rax Integration (#605): 0 of 4 test(s) reached the change; crash, predates #605, since fixed (recorded); crash, introduced and since fixed (recorded)
- [valkey-io/valkey-search#614](https://github.com/valkey-io/valkey-search/pull/614) ([tests](tests/valkey-search/614-ae0f8f095/)): Stemming Ingestion and Search  (resolves #651) (#614): 4 of 4 test(s) reached the change; crash, predates #614, since fixed (recorded)
- [valkey-io/valkey-search#647](https://github.com/valkey-io/valkey-search/pull/647) ([tests](tests/valkey-search/647-e9571671c/)): Move MINSTEMSIZE clause from per text field to per schema (#647): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#641](https://github.com/valkey-io/valkey-search/pull/641) ([tests](tests/valkey-search/641-2fc6219c2/)): Revert "Update Vector HNSW output field format (#312)" (#641): 2 of 2 test(s) reached the change; crash, predates #641, since fixed (recorded)

## Totals

- Merges whose tests reached the change: 405 of 576 tested (70%)
- Changed-line coverage: median 100% of 265 merge(s) measured, 16 under 50%
- Recall: 120 parents flagged of 168 fix merges
- Claude: $1631.34 in the last 24 hours, $4014.34 in the last 7 days, $4014.34 in all
- Issues filed: 42
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/40
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/41
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/42
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/43
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/44
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/45
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/47
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/48
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/49
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/50
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/53
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/54
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/55
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/56
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/57
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/58
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/59
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/60
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/61
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/62
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/63
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/64
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/65
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/66
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/67
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/68
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/70
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/72
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/73
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/75
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/76
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/78
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/79
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/80
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/81
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/82
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/83
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/84
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/85
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/86
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/87
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/89

## Retries

- [valkey-io/valkey-search#741](https://github.com/valkey-io/valkey-search/pull/741): attempt 2 of 3, `deps=/opt/valkey-search-deps` failed in parent (exit 1): [138/223] Building CXX object src/commands/CMakeFiles/commands.dir/commands.cc.o FAILED: [code=1] src/commands/CMakeFiles/commands.dir/command
- [valkey-io/valkey-search#735](https://github.com/valkey-io/valkey-search/pull/735): attempt 2 of 3, `deps=/opt/valkey-search-deps` failed in merge (exit 1): [138/223] Building CXX object src/commands/CMakeFiles/commands.dir/commands.cc.o FAILED: [code=1] src/commands/CMakeFiles/commands.dir/commands
- [valkey-io/valkey-search#734](https://github.com/valkey-io/valkey-search/pull/734): attempt 2 of 3, `deps=/opt/valkey-search-deps` failed in merge (exit 1): [137/221] Building CXX object src/commands/CMakeFiles/commands.dir/commands.cc.o FAILED: [code=1] src/commands/CMakeFiles/commands.dir/commands
- [valkey-io/valkey-search#621](https://github.com/valkey-io/valkey-search/pull/621): attempt 2 of 3, `deps=/opt/valkey-search-deps` failed in merge (exit 1): [137/221] Building CXX object src/commands/CMakeFiles/commands.dir/commands.cc.o FAILED: [code=1] src/commands/CMakeFiles/commands.dir/commands
- [valkey-io/valkey#3800](https://github.com/valkey-io/valkey/pull/3800): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#3613](https://github.com/valkey-io/valkey/pull/3613): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#3578](https://github.com/valkey-io/valkey/pull/3578): attempt 1 of 3, 3 of 3 run(s) could not start: the run could not start: environment acquisition failed
