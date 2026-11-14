# Fuzzer walk

Updated 2026-11-14T13:37+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 2369 | 171 | #12906 (2024-01-15) | 301 | 247.3 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 0.0 |
| valkey-io/valkey-json | 66 | 0 | c1423d7e4b38 (2024-11-25) | 2 | 0.6 |
| valkey-io/valkey-search | 690 | 0 | f8106e72e984 (2024-11-27) | 9 | 36.4 |

## Last runs

### valkey-io/valkey

[2026-11-14T13:20:24+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43586358252): 20 merge(s) tested, 4 without a code change.

- [redis/redis#12899](https://github.com/redis/redis/pull/12899) ([tests](tests/valkey/12899-5358bd7cd/)): Reduce performance impact of dict rehashing and make it shorter. (#12899): 0 of 5 test(s) reached the change, coverage 98%
- [redis/redis#12993](https://github.com/redis/redis/pull/12993) ([tests](tests/valkey/12993-98881f755/)): fix the wrong path in mkreleasehdr.sh (#12993): declined: The change fixes a path in src/mkreleasehdr.sh, the build script that computes the GIT_DIRTY flag embedded in release.h at compile time. It is a build/CI-only change with no effect on server runtime behavior, command replies, replication, persistence, or the bundled tools, so no fuzzer sce
- [redis/redis#12989](https://github.com/redis/redis/pull/12989) ([tests](tests/valkey/12989-4cb5ad85a/)): Fix unauthenticated client query buffer 1MB limit (#12989): declined: The change only lowers a dead constant. The fixed branch (networking.c:2729) that caps an unauthenticated client's query buffer applies solely when authRequired(c) is true, but that same gate already enforces strictly tighter parser limits present in both builds: unauthenticated multibulk 
- [redis/redis#12961](https://github.com/redis/redis/pull/12961) ([tests](tests/valkey/12961-85a834bfa/)): Revert multi OOM limit and add multi buffer limit (#12961): 2 of 2 test(s) reached the change, coverage 100%
- [redis/redis#12980](https://github.com/redis/redis/pull/12980) ([tests](tests/valkey/12980-07b292af5/)): Add sender NULL check in clusterProcessGossipSection invalid_ids case (#12980): declined: The fix guards a NULL-pointer dereference in clusterProcessGossipSection that occurs only inside the invalid_ids branch: when a node receives a cluster-bus gossip section containing invalid (non-[a-z0-9]) node IDs from a sender it does not yet know (link->node NULL and clusterLookupNode(hd
- [redis/redis#12977](https://github.com/redis/redis/pull/12977) ([tests](tests/valkey/12977-85c31e0cf/)): Allow running WAITAOF in scripts, remove NOSCRIPT flag (#12977): 3 of 3 test(s) reached the change
- [redis/redis#12940](https://github.com/redis/redis/pull/12940) ([tests](tests/valkey/12940-628c0dea1/)): Some cleanups around function (#12940): 2 of 5 test(s) reached the change, coverage 88%; crash, introduced and since fixed (recorded); crash, fixed by redis/redis#12940 (recorded); crash, introduced and since fixed (recorded); crash, fixed by redis/redis#12940 (recorded); crash, introduced and since fixed (recorded)
- [redis/redis#12969](https://github.com/redis/redis/pull/12969) ([tests](tests/valkey/12969-f9a0eb60f/)): update redis-check-rdb types (#12969): 0 of 1 test(s) reached the change
- [redis/redis#12965](https://github.com/redis/redis/pull/12965) ([tests](tests/valkey/12965-2bce71b5f/)): Exit early if slowlog/acllog max len set to zero (#12965): 2 of 2 test(s) reached the change, coverage 100%
- [redis/redis#12921](https://github.com/redis/redis/pull/12921) ([tests](tests/valkey/12921-e12f2decc/)): Prevent nodes with invalid IDs from being propagated through gossip (#12921): 0 of 2 test(s) reached the change, coverage 36%
- [redis/redis#12947](https://github.com/redis/redis/pull/12947) ([tests](tests/valkey/12947-8d0156eb1/)): Set the correct id for tempDb (#12947): 0 of 3 test(s) reached the change, coverage 100%
- [redis/redis#12931](https://github.com/redis/redis/pull/12931) ([tests](tests/valkey/12931-85a239b36/)): Change dictGetSafeIterator to dictGetIterator in pubsub (#12931): 0 of 3 test(s) reached the change, coverage 100%
- [redis/redis#12948](https://github.com/redis/redis/pull/12948) ([tests](tests/valkey/12948-b07174afc/)): Change the threshold of dict expand, shrink and rehash (#12948): 1 of 4 test(s) reached the change, coverage 100%
- [redis/redis#12817](https://github.com/redis/redis/pull/12817) ([tests](tests/valkey/12817-d0640029d/)): Fix race condition issues between the main thread and module threads (#12817): 6 of 18 test(s) reached the change, coverage 60%
- [redis/redis#12950](https://github.com/redis/redis/pull/12950): to retry, Claude Code failed (rc=1): timeout after 600s
- [redis/redis#12962](https://github.com/redis/redis/pull/12962) ([tests](tests/valkey/12962-1c7eb0ad3/)): Fix minor memory leaks in dictTest (#12962): declined: The change touches only dictTest() in src/dict.c, the internal unit-test harness code run via the test runner, not any server runtime path. It frees temporary strings allocated by stringFromLongLong inside the test to silence test-only memory leaks; no command, reply, data structure, repli
- [redis/redis#12510](https://github.com/redis/redis/pull/12510) ([tests](tests/valkey/12510-0e5a4a27e/)): Call emptyData when disk-based sync rdbLoad fails (#12510): 0 of 4 test(s) reached the change, coverage 0%
- [redis/redis#12952](https://github.com/redis/redis/pull/12952) ([tests](tests/valkey/12952-14b1edfd9/)): Fix dict resize ratio checks, avoid precision loss from integer division (#12952): 2 of 4 test(s) reached the change, coverage 85%
- [redis/redis#12946](https://github.com/redis/redis/pull/12946) ([tests](tests/valkey/12946-ecc31bc69/)): Updated comments on dictResizeEnable for new dict shrink (#12946): declined: The diff changes only a code comment in src/dict.c (updating the documentation of dict_can_resize to mention the new dict shrink condition). No executable code, config, or behaviour changes, so no scenario can exercise it.
- [redis/redis#12850](https://github.com/redis/redis/pull/12850) ([tests](tests/valkey/12850-e2b7932b3/)): Shrink dict when deleting dictEntry (#12850): 3 of 6 test(s) reached the change, coverage 86%

### valkey-io/valkey-bloom

[2026-11-14T13:36:10+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43600541177): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-14T13:37:18+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43600632753): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-14T13:10:45+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43598362962): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 940 of 1501 tested (63%)
- Changed-line coverage: median 100% of 985 merge(s) measured, 76 under 50%
- Recall: 247 parents flagged of 390 fix merges
- Claude: $694.07 in the last 24 hours, $6446.61 in the last 7 days, $11002.32 in all
- Issues filed: 79
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
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/145
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/146
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/147
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/153
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/156
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/157
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
- Model updates: 0 asked for, 0 with no model change, 0 rejected, 0 failed; 0 pull request(s)

## Retries

- [redis/redis#12950](https://github.com/redis/redis/pull/12950): attempt 1 of 3, Claude Code failed (rc=1): timeout after 600s
