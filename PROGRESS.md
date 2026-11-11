# Fuzzer walk

Updated 2026-11-11T07:44+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 1697 | 825 | #1160 (2024-10-18) | 254 | 242.4 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 64 | 0 | c1423d7e4b38 (2024-11-25) | 1 | 9.1 |
| valkey-io/valkey-search | 672 | 0 | f8106e72e984 (2024-11-27) | 0 | 96.0 |

## Last runs

### valkey-io/valkey

[2026-11-11T05:47:46+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42957723574): 20 merge(s) tested, 0 without a code change.

- [valkey-io/valkey#1239](https://github.com/valkey-io/valkey/pull/1239) ([tests](tests/valkey/1239-13f5f665f/)): Update the argument of clusterNodeGetReplica declaration (#1239): declined: The change only renames a parameter (slave_idx to replica_idx) in the clusterNodeGetReplica function declaration in src/cluster.h. A parameter name in a C declaration has no effect on compiled code or behavior; this is a pure naming cleanup with nothing to test.
- [valkey-io/valkey#1232](https://github.com/valkey-io/valkey/pull/1232) ([tests](tests/valkey/1232-5a4c0640c/)): Mark main and serverAssert as weak symbols to be overridden (#1232): declined: The change only adds __attribute__((weak)) to main and _serverAssert and drops the --allow-multiple-definition linker flag so the unit-test binary can supply its own main/assert handler; in valkey-server itself no overriding strong symbol exists, so the linked code and all runtime behavior
- [valkey-io/valkey#1233](https://github.com/valkey-io/valkey/pull/1233) ([tests](tests/valkey/1233-8ee7a5802/)): Document log format configs in valkey.conf (#1233): declined: The change only adds comment lines to valkey.conf documenting the existing log-format and log-timestamp-format configs. All added lines are commented out (# log-format legacy, # log-timestamp-format legacy), so no server behaviour, config parsing, or logging output changes; there is nothin
- [valkey-io/valkey#1220](https://github.com/valkey-io/valkey/pull/1220) ([tests](tests/valkey/1220-c21f1dc08/)): Increase the IO_THREADS_MAX_NUM. (#1220): 2 of 4 test(s) reached the change
- [valkey-io/valkey#1226](https://github.com/valkey-io/valkey/pull/1226) ([tests](tests/valkey/1226-5d2ff853a/)): Fix minor repldbfd leak in updateReplicasWaitingBgsave if fstat fails (#1226): 0 of 3 test(s) reached the change, coverage 44%
- [valkey-io/valkey#1224](https://github.com/valkey-io/valkey/pull/1224) ([tests](tests/valkey/1224-4be09e434/)): Fix typo in valkey.conf file's shutdown section (#1224): declined: The change only fixes a typo in a comment in valkey.conf (the shutdown-timeout documentation: 'before the primary exists' → 'before the primary exits'). It touches no code, no default values, and no parsing behavior — comments are ignored by the config loader — so there is no runtime behav
- [valkey-io/valkey#1212](https://github.com/valkey-io/valkey/pull/1212) ([tests](tests/valkey/1212-9c60fcdae/)): Do security attack check only when command not found to reduce the critical path (#1212): 0 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1022](https://github.com/valkey-io/valkey/pull/1022) ([tests](tests/valkey/1022-55bbbe09a/)): Configurable log and timestamp formats (logfmt, ISO8601) (#1022): 1 of 6 test(s) reached the change, coverage 98%
- [valkey-io/valkey#1222](https://github.com/valkey-io/valkey/pull/1222) ([tests](tests/valkey/1222-295636773/)): Maintain return value of rdbSaveDb after writing slot-info aux (#1222): declined: The change only adds `written += res;` so rdbSaveDb's returned byte count includes the slot-info aux field. rdbSaveDb has a single caller (rdbSaveRio, src/rdb.c:1423) which only checks the return value for -1 (error); the non-negative byte count is never read by any code path. The bytes wr
- [valkey-io/valkey#1189](https://github.com/valkey-io/valkey/pull/1189) ([tests](tests/valkey/1189-a21fe718f/)): Limit CLUSTER_CANT_FAILOVER_DATA_AGE log to 10 times period (#1189): declined: The change only rate-limits one serverLog() notice in clusterLogCantFailover(): when a replica is repeatedly blocked from failover by CLUSTER_CANT_FAILOVER_DATA_AGE, the log is emitted every 10 seconds instead of every 1. It is logging-only: the new branch is a single comparison plus retur
- [valkey-io/valkey#1213](https://github.com/valkey-io/valkey/pull/1213) ([tests](tests/valkey/1213-c419524c0/)): Trim free space from inline command argument strings to avoid excess memory usage (#1213): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1214](https://github.com/valkey-io/valkey/pull/1214) ([tests](tests/valkey/1214-c176de425/)): Clarify the wording from dually to the more common doubly (#1214): declined: The change is a one-word wording fix in a comment at the top of src/ziplist.c ("dually linked list" -> "doubly linked list"). It modifies no code paths, configuration, replies, or build artifacts, so there is no behaviour to exercise and no scenario is needed.
- [valkey-io/valkey#1211](https://github.com/valkey-io/valkey/pull/1211) ([tests](tests/valkey/1211-b803f7aef/)): Cleaned up getSlotOrReply is return -1 instead of C_ERR (#1211): declined: Pure cosmetic refactor with no behavior change: C_ERR is '#define C_ERR -1' (src/server.h:112), so replacing '== C_ERR' with '== -1' at the getSlotOrReply call sites in cluster_legacy.c and cluster_slot_stats.c compiles to the identical comparison. The function already returned the literal
- [valkey-io/valkey#1190](https://github.com/valkey-io/valkey/pull/1190) ([tests](tests/valkey/1190-5d70ccd70/)): Make replica CLUSTER RESET flush async based on lazyfree-lazy-user-flush (#1190): 4 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1202](https://github.com/valkey-io/valkey/pull/1202) ([tests](tests/valkey/1202-285064b11/)): fix typo  (#1202): declined: The change edits only a comment in src/io_threads.c, correcting the typo 'areadty' to 'already' in trySendReadToIOThreads. No executable code, config, protocol, or build file changes; behaviour is identical on both builds, so no scenario is needed.
- [valkey-io/valkey#1203](https://github.com/valkey-io/valkey/pull/1203) ([tests](tests/valkey/1203-771918e4b/)): Updating command.def by running the generate-command-code.py  (#1203): 0 of 2 test(s) reached the change
- [valkey-io/valkey#1200](https://github.com/valkey-io/valkey/pull/1200) ([tests](tests/valkey/1200-5885dc56b/)): Fix BGSAVE CANCEL since and history fields (#1200): 2 of 2 test(s) reached the change
- [valkey-io/valkey#757](https://github.com/valkey-io/valkey/pull/757) ([tests](tests/valkey/757-29b83f1ac/)): Introduce bgsave cancel (#757): 3 of 5 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1194](https://github.com/valkey-io/valkey/pull/1194) ([tests](tests/valkey/1194-71f8c34ee/)): RDMA: Fix listener priv opaque pointer (#1194): declined: The change is confined to src/rdma.c, which is compiled only when Valkey is built with BUILD_RDMA=module (default is BUILD_RDMA=no), so the fuzzer's builds do not contain the changed code at all. Even on an RDMA-enabled build, reaching rdmaFdToListener/rdmaChangeListener requires an RDMA d
- [valkey-io/valkey#1182](https://github.com/valkey-io/valkey/pull/1182) ([tests](tests/valkey/1182-2743b7e04/)): Fix SORT GET to ignore special pattern # in cluster slot check (#1182): 2 of 2 test(s) reached the change, coverage 100%

### valkey-io/valkey-bloom

[2026-11-11T07:44:18+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42981599314): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-11T07:29:35+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42979801685): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-11T07:32:31+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42980146854): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 786 of 1240 tested (63%)
- Changed-line coverage: median 100% of 764 merge(s) measured, 61 under 50%
- Recall: 211 parents flagged of 333 fix merges
- Claude: $876.67 in the last 24 hours, $9010.02 in the last 7 days, $9010.02 in all
- Issues filed: 68
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

- [valkey-io/valkey#1535](https://github.com/valkey-io/valkey/pull/1535): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#1310](https://github.com/valkey-io/valkey/pull/1310): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
