# Fuzzer walk

Updated 2026-11-12T08:34+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 1971 | 559 | #638 (2024-06-16) | 248 | 272.4 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 11.6 |
| valkey-io/valkey-json | 64 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 2.3 |
| valkey-io/valkey-search | 673 | 0 | f8106e72e984 (2024-11-27) | 1 | 92.7 |

## Last runs

### valkey-io/valkey

[2026-11-12T08:34:57+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43160012695): 20 merge(s) tested, 3 without a code change.

- [valkey-io/valkey#1310](https://github.com/valkey-io/valkey/pull/1310) ([tests](tests/valkey/1310-3d0c83420/)): Fix LRU crash when getting too many random lua scripts (#1310): 0 of 1 test(s) reached the change, coverage 100%
- [valkey-io/valkey#351](https://github.com/valkey-io/valkey/pull/351) ([tests](tests/valkey/351-1269532fb/)): Introduce CLUSTER SLOT-STATS command (#20). (#351): 7 of 7 test(s) reached the change, coverage 99%
- [valkey-io/valkey#704](https://github.com/valkey-io/valkey/pull/704) ([tests](tests/valkey/704-7719dbb84/)): Update readonly and readwrite json (#704): 2 of 2 test(s) reached the change
- [valkey-io/valkey#700](https://github.com/valkey-io/valkey/pull/700) ([tests](tests/valkey/700-ad5704f80/)): Upstream the availability zone info string from KeyDB (#700): 2 of 2 test(s) reached the change
- [valkey-io/valkey#701](https://github.com/valkey-io/valkey/pull/701) ([tests](tests/valkey/701-2b0723957/)): Enable protected-configs, debug and module commands in create-cluster script (#701): declined: The change edits only utils/create-cluster/create-cluster, a developer helper shell script, adding --enable-protected-configs, --enable-debug-command and --enable-module-command to the valkey-server launch line for locally created test clusters. No server, valkey-cli, valkey-benchmark or c
- [valkey-io/valkey#325](https://github.com/valkey-io/valkey/pull/325) ([tests](tests/valkey/325-28c5a17ed/)): replica redirect read&write to primary in standalone mode (#325): 5 of 6 test(s) reached the change, coverage 100%
- [valkey-io/valkey#691](https://github.com/valkey-io/valkey/pull/691) ([tests](tests/valkey/691-ab3873011/)): Replacing REDIS_STATIC with static (#691): declined: The change only renames the REDIS_STATIC macro to the literal keyword static in src/quicklist.c and removes the now-unused -DREDIS_STATIC=''/-DSERVER_STATIC='' definitions from the Makefiles. quicklist.c's fallback already defined REDIS_STATIC as static (the Makefile defined SERVER_STATIC,
- [valkey-io/valkey#689](https://github.com/valkey-io/valkey/pull/689) ([tests](tests/valkey/689-495c35d91/)): Add check in CLUSTERLINK KILL cmd to avoid freeing links to myself (#689): 1 of 1 test(s) reached the change, coverage 100%
- [valkey-io/valkey#694](https://github.com/valkey-io/valkey/pull/694) ([tests](tests/valkey/694-3df9d4279/)): Fix bad memory accounting for sds when no malloc_size available (#694): declined: The change swaps s_malloc_size for s_malloc_usable_size in sdsAllocSize for SDS_TYPE_5 strings. These differ only when HAVE_MALLOC_SIZE is undefined, i.e. a build compiled with NO_MALLOC_USABLE_SIZE on a libc without malloc_usable_size/malloc_size; on every default Linux build (jemalloc, t
- [valkey-io/valkey#670](https://github.com/valkey-io/valkey/pull/670) ([tests](tests/valkey/670-4d3d6c06a/)): Reduce redundant call of prepareClientToWrite when call addReply* continuously. (#670): 0 of 3 test(s) reached the change, coverage 91%
- [valkey-io/valkey#686](https://github.com/valkey-io/valkey/pull/686) ([tests](tests/valkey/686-32ca6e5b3/)):  Improve `CLUSTER SETSLOT` replication handling to support older replica versions. (#686): 0 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#453](https://github.com/valkey-io/valkey/pull/453) ([tests](tests/valkey/453-0143b7c9d/)): Add zfree_with_size to optimize sdsfree since we can get zmalloc_size from the header (#453): 0 of 5 test(s) reached the change, coverage 100%; crash, predates #453, since fixed (recorded)
- [valkey-io/valkey#675](https://github.com/valkey-io/valkey/pull/675) ([tests](tests/valkey/675-5d2348cee/)): Update json file and sentinelCommand function for Valkey Sentinel  (#675): 2 of 2 test(s) reached the change
- [valkey-io/valkey#676](https://github.com/valkey-io/valkey/pull/676) ([tests](tests/valkey/676-e84eda909/)): Remove useless code in sentinel source code (#676): declined: The change only deletes an empty conditional block in sentinelHandleRedisInstance (an `if (ri->flags & (SRI_PRIMARY | SRI_REPLICA))` whose body was just the comment 'Nothing so far.'). The condition had no side effects and the body was empty, so the compiled behavior is identical on both b
- [valkey-io/valkey#617](https://github.com/valkey-io/valkey/pull/617) ([tests](tests/valkey/617-ae2d4217e/)): Add new SCRIPT SHOW subcommand to dump script via sha1 (#617): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#665](https://github.com/valkey-io/valkey/pull/665) ([tests](tests/valkey/665-be2c32168/)): Support RDB compatability with Redis 7.2.4 RDB format (#665): 2 of 6 test(s) reached the change, coverage 64%; [check, introduced by #665](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/128); [crash, introduced by #665](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/129)
- [valkey-io/valkey#658](https://github.com/valkey-io/valkey/pull/658) ([tests](tests/valkey/658-a2cc2fe26/)): Fix memory leak when loading slot migrations states fails (#658): declined: The change only frees two robjs on an error branch in rdbLoadRioWithLoadingCtx: when a registered dynamic RDB aux-field decoder returns C_ERR. The sole registered decoder, clusterDecodeOpenSlotsAuxField (cluster-slot-states), runs only during a cluster full sync (RDBFLAGS_REPLICATION) and 
- [valkey-io/valkey#662](https://github.com/valkey-io/valkey/pull/662) ([tests](tests/valkey/662-b33f932c5/)): Add missing commas from debug command (#662): 2 of 2 test(s) reached the change; [check, appeared after #662](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/128); [crash, appeared after #662](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/129)
- [valkey-io/valkey#660](https://github.com/valkey-io/valkey/pull/660) ([tests](tests/valkey/660-4135894a5/)): Update remaining `master` references to `primary` (#660): 5 of 18 test(s) reached the change, coverage 67%; [check, appeared after #660](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/128); [crash, appeared after #660](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/129)
- [valkey-io/valkey#633](https://github.com/valkey-io/valkey/pull/633) ([tests](tests/valkey/633-495a121f1/)): Adjust the log level of some logs in the cluster (#633): declined: The change only raises the log level (LL_VERBOSE/LL_DEBUG to LL_NOTICE) and slightly rewords four serverLog messages in src/cluster_legacy.c (configEpoch collision, gossip failure-report add/remove, mismatching PONG sender ID, node possibly failing). No control flow, cluster state, command

### valkey-io/valkey-bloom

[2026-11-12T08:04:08+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43179864596): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-12T08:03:46+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43179806979): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-12T07:37:39+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43176563592): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 850 of 1361 tested (62%)
- Changed-line coverage: median 100% of 868 merge(s) measured, 66 under 50%
- Recall: 225 parents flagged of 356 fix merges
- Claude: $852.53 in the last 24 hours, $9469.96 in the last 7 days, $9947.08 in all
- Issues filed: 73
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/100
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/101
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/102
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/103
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/104
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/105
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/106
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/107
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/108
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/109
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/110
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/111
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/112
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/113
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/114
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/115
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/117
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/122
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/125
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/126
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/127
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/128
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/129
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
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/90
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/91
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/92
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/93
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/94
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/95
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/96
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/98

## Retries

None.
