# Fuzzer walk

Updated 2026-11-14T19:45+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 2439 | 102 | #12738 (2023-11-27) | 294 | 240.3 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 0.0 |
| valkey-io/valkey-json | 66 | 0 | c1423d7e4b38 (2024-11-25) | 2 | 0.6 |
| valkey-io/valkey-search | 691 | 0 | f8106e72e984 (2024-11-27) | 9 | 30.0 |

## Last runs

### valkey-io/valkey

[2026-11-14T19:27:48+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43625621113): 20 merge(s) tested, 3 without a code change.

- [redis/redis#12847](https://github.com/redis/redis/pull/12847) ([tests](tests/valkey/12847-15b993f1e/)): Optimize dictExpand of empty dict (#12847): 0 of 3 test(s) reached the change, coverage 100%
- [redis/redis#12823](https://github.com/redis/redis/pull/12823) ([tests](tests/valkey/12823-826b39e01/)): Align server.lastsave and server.rdb_save_time_last by removing multiple calls to time(NULL) (#12823): declined: The change is a timekeeping consistency refactor in backgroundSaveDoneHandler: one time(NULL) sample at handler entry now sets both server.lastsave and server.rdb_save_time_last, instead of two separate calls. No control flow, persistence format, replication behavior, or reply shape change
- [redis/redis#12843](https://github.com/redis/redis/pull/12843) ([tests](tests/valkey/12843-f2d59c4f9/)): Avoid unnecessary slot computing in KEYS command. (#12843): declined: The change only moves the patternHashSlot computation in keysCommand behind the existing server.cluster_enabled guard. pslot is initialized to -1 and is now assigned only when cluster_enabled && !allkeys, so the new condition `pslot != -1` selects the single-slot iterator under exactly the
- [redis/redis#12839](https://github.com/redis/redis/pull/12839) ([tests](tests/valkey/12839-8e11f84de/)): Fix replica node cannot expand dicts when loading legacy RDB (#12839): declined: The change is a hash-table pre-sizing optimization on a cold path with no externally observable behavior change, so no fuzzer scenario can drive it meaningfully. The only edit is the divisor used by dbExpand in cluster mode: getMyClusterSlotCount() (a replica's own numslots, always 0) beco
- [redis/redis#12244](https://github.com/redis/redis/pull/12244) ([tests](tests/valkey/12244-2f6d4daba/)): Fix outdated LFU comments to eliminate confusion (#12244): declined: The change edits only comments in src/evict.c (correcting the LFU field description from 'last decr time' to 'last access time' and describing the current decay-by-one behavior). No executable code, replies, configs, or data paths change, so no scenario can observe any difference and none 
- [redis/redis#12840](https://github.com/redis/redis/pull/12840) ([tests](tests/valkey/12840-b730404c2/)): Fix multi dbs donot dbExpand when loading RDB (#12840): declined: The change only flips when rdbLoadRioWithLoadingCtx pre-sizes a database's hash tables during RDB load: the old code armed should_expand_db once at entry, so only the first database in a multi-DB RDB got its dbExpand() and every subsequent SELECTDB's RESIZEDB hint was ignored; the fix re-a
- [redis/redis#12781](https://github.com/redis/redis/pull/12781) ([tests](tests/valkey/12781-9ee1cc33a/)): Make the sampling logic in eviction clearer (#12781): 0 of 7 test(s) reached the change, coverage 100%
- [redis/redis#12754](https://github.com/redis/redis/pull/12754) ([tests](tests/valkey/12754-991aff1c0/)): Optimize KEYS when pattern includes hashtag and implies a single slot. (#12754): 0 of 3 test(s) reached the change, coverage 100%
- [redis/redis#12799](https://github.com/redis/redis/pull/12799) ([tests](tests/valkey/12799-35c8d616c/)): Only rebuild server when src or deps are modified (#12799): declined: The change edits only src/mkreleasehdr.sh, narrowing the GIT_DIRTY check to `git diff -- src deps` so that modifications outside src/ and deps/ no longer mark the build dirty and force a rebuild. This is purely a build/CI convenience change: it does not alter any server behavior, command r
- [redis/redis#12829](https://github.com/redis/redis/pull/12829) ([tests](tests/valkey/12829-764838d66/)): Check whether the client is NULL in luaCreateFunction (#12829): declined: The change only adds a NULL-client guard in luaCreateFunction's shebang-error path (src/eval.c). Both callers in this tree, evalGenericCommand (eval.c:512) and SCRIPT LOAD (eval.c:626), always pass a non-NULL client, and the old NULL-client caller (script reload from the RDB 'lua' aux fiel
- [redis/redis#12726](https://github.com/redis/redis/pull/12726) ([tests](tests/valkey/12726-8a4ccb01b/)): Fix clusterLoadConfig aux_argv minor memory leak (#12726): declined: The change only adds sdsfreesplitres(aux_argv, ...) on error paths in clusterLoadConfig that immediately 'goto fmterr', which logs and calls exit(1). The memory being freed was reclaimed by process exit anyway, so there is no observable behaviour change on either build: the paths are reach
- [redis/redis#12786](https://github.com/redis/redis/pull/12786) ([tests](tests/valkey/12786-91309f798/)): Fix compilation warning in KeySpace_ServerEventCallback and add CFLAGS=-Werror flag for module CI (#12786): declined: The change is a CI/build change plus a cosmetic fix in a test module: it adds CFLAGS=-Werror to the module-API test steps in the GitHub workflows and fixes a RedisModule_Log format specifier (%ld -> %llu with a cast) in tests/modules/postnotifications.c. No server code, command reply, repl
- [redis/redis#12641](https://github.com/redis/redis/pull/12641) ([tests](tests/valkey/12641-e216c8390/)): Change addReplyErrorFormat to addReplyError when there is no format (#12641): declined: The change swaps addReplyErrorFormat for addReplyError at four call sites whose message is a plain literal with no % conversion specifiers and no newlines. Both functions emit the identical error reply (addReplyErrorLength + afterErrorReply with flags 0); addReplyErrorFormat only adds a pr
- [redis/redis#12818](https://github.com/redis/redis/pull/12818) ([tests](tests/valkey/12818-423565f78/)): Optimize CPU cache efficiency on dict while rehashing in dictTwoPhaseUnlinkFind (#12818): 0 of 3 test(s) reached the change, coverage 100%
- [redis/redis#12815](https://github.com/redis/redis/pull/12815) ([tests](tests/valkey/12815-22cc9b512/)): Use CLZ in _dictNextExp to get the next power of two (#12815): declined: The change replaces the shift loop in the static helper _dictNextExp (src/dict.c) with a __builtin_clzl computation. The new expression 8*sizeof(long) - clzl(size-1) equals the old loop's result (the smallest e >= 2 with 2^e >= size) for every input: sizes 0-4 hit the new DICT_HT_INITIAL_E
- [redis/redis#12768](https://github.com/redis/redis/pull/12768) ([tests](tests/valkey/12768-bdceaf50e/)): Fix the check for new RDB_OPCODE_SLOT_INFO in redis-check-rdb (#12768): 0 of 3 test(s) reached the change, coverage 0%
- [redis/redis#7351](https://github.com/redis/redis/pull/7351) ([tests](tests/valkey/7351-3431b1f15/)): format cpu config as redis style (#7351): 2 of 2 test(s) reached the change
- [redis/redis#12802](https://github.com/redis/redis/pull/12802) ([tests](tests/valkey/12802-a1c5171c1/)): Fix resize hash tables stuck on the last non-empty slot (#12802): 3 of 4 test(s) reached the change, coverage 100%
- [redis/redis#12811](https://github.com/redis/redis/pull/12811) ([tests](tests/valkey/12811-095d05786/)): clarify the comment of findSlotByKeyIndex function (#12811): declined: The change (#12811) only edits comments in src/db.c: it clarifies the doc comment of findSlotByKeyIndex and expands the inline comment explaining why the +1 and -1 adjustments cancel out. No executable code is modified, so there is no behaviour change to exercise and no scenario is needed.
- [redis/redis#12809](https://github.com/redis/redis/pull/12809) ([tests](tests/valkey/12809-d6f19539d/)): Un-register notification and server event when RedisModule_OnLoad fails (#12809): declined: The change fixes a real use-after-free in moduleLoad()'s OnLoad-failure branch (dangling keyspace-notification subscribers, server-event callbacks, command filters, and authenticated clients after the module struct is freed; moduleUnload() itself is only refactored, no behavior change). Bu

### valkey-io/valkey-bloom

[2026-11-14T19:45:10+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43633722312): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-14T19:20:39+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43631524731): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-14T18:52:00+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43628892454): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 957 of 1533 tested (62%)
- Changed-line coverage: median 100% of 1014 merge(s) measured, 78 under 50%
- Recall: 252 parents flagged of 397 fix merges
- Claude: $862.25 in the last 24 hours, $6233.87 in the last 7 days, $11265.01 in all
- Issues filed: 80
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
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/162
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

None.
