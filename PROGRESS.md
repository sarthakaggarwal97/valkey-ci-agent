# Fuzzer walk

Updated 2026-11-14T23:51+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 2490 | 51 | #11695 (2023-10-14) | 321 | 239.7 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 0.0 |
| valkey-io/valkey-json | 66 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 0.6 |
| valkey-io/valkey-search | 692 | 0 | f8106e72e984 (2024-11-27) | 7 | 26.7 |

## Last runs

### valkey-io/valkey

[2026-11-14T23:19:07+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43643733893): 20 merge(s) tested, 9 without a code change.

- [redis/redis#12667](https://github.com/redis/redis/pull/12667) ([tests](tests/valkey/12667-88e83e517/)): Add DEBUG_ASSERTIONS option to custom assert (#12667): declined: The change only adds a debugServerAssertWithInfo macro that is a no-op unless the build is compiled with -DDEBUG_ASSERTIONS, plus CI workflow edits enabling that flag in GitHub Actions sanitizer jobs. In the builds under test the new assertion in getKeySlot expands to nothing, so the compi
- [redis/redis#12750](https://github.com/redis/redis/pull/12750) ([tests](tests/valkey/12750-6258edebf/)): reset bucket_count when empty db (#12750): 3 of 3 test(s) reached the change, coverage 100%
- [redis/redis#12741](https://github.com/redis/redis/pull/12741) ([tests](tests/valkey/12741-cf6ed3fee/)): fix the wrong judgement for activerehashing in standalone mode (#12741): 3 of 3 test(s) reached the change, coverage 100%
- [redis/redis#12739](https://github.com/redis/redis/pull/12739) ([tests](tests/valkey/12739-53294e537/)): Fix genClusterDebugString minor sds leaks (#12739): declined: The change only adds sdsfree calls in genClusterDebugString, which is reachable solely from printCrashReport via logServerInfo while the server is already crashing (assert, panic, or fatal signal handler). It fixes a tiny sds leak in a process moments from aborting; the crash-report output
- [redis/redis#12733](https://github.com/redis/redis/pull/12733) ([tests](tests/valkey/12733-0ffb9d2ea/)): Before evicted and before expired server events are not executed inside an execution unit. (#12733): 0 of 3 test(s) reached the change, coverage 100%
- [redis/redis#12722](https://github.com/redis/redis/pull/12722) ([tests](tests/valkey/12722-a888503b4/)): Remove unnecessary argument(tp) in gettimeofday() call for retrieving timezone (#12722): declined: The change edits only the non-Linux/non-Solaris branch of getTimeZone() in src/util.c, replacing gettimeofday(&tv, &tz) with gettimeofday(NULL, &tz). On Linux (where the fuzzer runs) this code is excluded by the preprocessor, so the builds under test are byte-for-byte equivalent on this pa
- [redis/redis#12707](https://github.com/redis/redis/pull/12707) ([tests](tests/valkey/12707-282b82e9d/)): Handle all CLUSTER_REDIR_ error code when verifying script. (#12707): 2 of 2 test(s) reached the change, coverage 100%
- [redis/redis#11489](https://github.com/redis/redis/pull/11489) ([tests](tests/valkey/11489-28b6155ba/)): Fix the bug that write redis sensitive command information to redis_cli historyfile (#11489): declined: The change only alters which commands valkey-cli's interactive REPL withholds from its history file (isSensitiveCommand in src/redis-cli.c). That function's effect is gated on stdin being a tty: repl() enables history and resolves the history file only inside isatty(fileno(stdin)) (redis-c
- [redis/redis#12536](https://github.com/redis/redis/pull/12536) ([tests](tests/valkey/12536-8878817d8/)): Optimize SCAN with MATCH when pattern implies cluster slot (#12536): 2 of 2 test(s) reached the change, coverage 93%
- [redis/redis#12711](https://github.com/redis/redis/pull/12711) ([tests](tests/valkey/12711-e9f312e08/)): Change stat_client_qbuf_limit_disconnections to atomic. (#12711): declined: The change only converts the stat_client_qbuf_limit_disconnections counter from a plain long long to a redisAtomic long long, switching its single increment (readQueryFromClient), its reset (resetServerStats), and its INFO read (genRedisInfoString) to atomicIncr/atomicSet/atomicGet. It is 
- [redis/redis#12702](https://github.com/redis/redis/pull/12702) ([tests](tests/valkey/12702-8d675950e/)): Don't crash when adding a forgotten node to blacklist twice (#12702): 1 of 4 test(s) reached the change, coverage 50%
- [redis/redis#5692](https://github.com/redis/redis/pull/5692) ([tests](tests/valkey/5692-4bbb2b015/)): Optimize CPU cache efficiency on dict while it's being rehashed (#5692): 0 of 5 test(s) reached the change, coverage 100%
- [redis/redis#12704](https://github.com/redis/redis/pull/12704) ([tests](tests/valkey/12704-f7fa48115/)): Optimize finding the slot for a given key count in a fenwick tree (#12704): 0 of 2 test(s) reached the change, coverage 100%
- [redis/redis#12697](https://github.com/redis/redis/pull/12697) ([tests](tests/valkey/12697-4145d628b/)): Reduce dbBuckets operation time complexity from O(N) to O(1) (#12697): 2 of 8 test(s) reached the change, coverage 88%
- [redis/redis#12694](https://github.com/redis/redis/pull/12694) ([tests](tests/valkey/12694-7d68208a6/)): Reset later item flag after defrag later is done (#12694): 0 of 3 test(s) reached the change, coverage 100%
- [redis/redis#12693](https://github.com/redis/redis/pull/12693) ([tests](tests/valkey/12693-ba900f6cb/)): Fix fd leak causing deleted files to remain open and eat disk space (#12693): 0 of 3 test(s) reached the change, coverage 100%
- [redis/redis#12687](https://github.com/redis/redis/pull/12687) ([tests](tests/valkey/12687-372ea2187/)): Update comment around propagateDeletion (#12687): declined: The change only updates comments: it rewords the doc comment above propagateDeletion in src/db.c (expiry -> general implicit deletion wording) and adds an explanatory comment before moduleNotifyKeyspaceEvent in delKeysInSlot in src/cluster.c. No statements, data structures, configs or repl
- [redis/redis#12660](https://github.com/redis/redis/pull/12660) ([tests](tests/valkey/12660-f3bf8485d/)): Fix resize hash table dictionary iterator (#12660): 2 of 4 test(s) reached the change, coverage 100%
- [redis/redis#12671](https://github.com/redis/redis/pull/12671) ([tests](tests/valkey/12671-03345ddc7/)): Fix issue of listen before chmod on Unix sockets (CVE-2023-45145) (#12671): declined: The change only reorders chmod() before listen() in anetUnixServer (CVE-2023-45145), closing a microsecond-scale startup race where the Unix socket briefly listened with umask-default permissions before unixsocketperm was applied. TCP callers pass perm=0 and the chmod is guarded to AF_LOCA
- [redis/redis#12655](https://github.com/redis/redis/pull/12655) ([tests](tests/valkey/12655-d27c7413a/)): remove heap allocations from signal handlers. (#12655): 0 of 6 test(s) reached the change, coverage 93%

### valkey-io/valkey-bloom

[2026-11-14T23:51:22+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43655229595): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-14T23:42:38+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43653228491): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-14T21:46:15+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43631714931): 1 merge(s) tested, 0 without a code change.

- [valkey-io/valkey-search#923](https://github.com/valkey-io/valkey-search/pull/923) ([tests](tests/valkey-search/923-57ae2bcff/)): Implement metric to CPU consumption for remote query (#923): 4 of 4 test(s) reached the change

## Totals

- Merges whose tests reached the change: 973 of 1560 tested (62%)
- Changed-line coverage: median 100% of 1040 merge(s) measured, 78 under 50%
- Recall: 258 parents flagged of 405 fix merges
- Claude: $1017.12 in the last 24 hours, $6162.42 in the last 7 days, $11480.56 in all
- Issues filed: 81
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
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/164
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
