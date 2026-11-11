# Fuzzer walk

Updated 2026-11-11T00:09+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 1633 | 889 | #1298 (2024-11-21) | 310 | 233.3 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 64 | 0 | c1423d7e4b38 (2024-11-25) | 1 | 9.1 |
| valkey-io/valkey-search | 672 | 0 | f8106e72e984 (2024-11-27) | 2 | 96.0 |

## Last runs

### valkey-io/valkey

[2026-11-11T00:09:59+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42916522777): 20 merge(s) tested, 3 without a code change.

- [valkey-io/valkey#1209](https://github.com/valkey-io/valkey/pull/1209) ([tests](tests/valkey/1209-4695d118d/)): RDMA builtin support (#1209): 2 of 4 test(s) reached the change, coverage 33%
- [valkey-io/valkey#1368](https://github.com/valkey-io/valkey/pull/1368) ([tests](tests/valkey/1368-a939cb88e/)): Handle keyIsExpiredWithDictIndex to make it check for import mode (#1368): 3 of 5 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1326](https://github.com/valkey-io/valkey/pull/1326) ([tests](tests/valkey/1326-db7b7396f/)): Make KEYS can visit expired key in import-source state (#1326): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1198](https://github.com/valkey-io/valkey/pull/1198) ([tests](tests/valkey/1198-5d08149e7/)): Use fake client flag to replace not conn check (#1198): 2 of 6 test(s) reached the change, coverage 100%; crash, fixed by #1198 (recorded)
- [valkey-io/valkey#999](https://github.com/valkey-io/valkey/pull/999) ([tests](tests/valkey/999-9305b4914/)): Add tag for dual-channel logs (#999): declined: The change only adds a '<Dual Channel> ' prefix to dual-channel replication log messages via a new dualChannelServerLog macro (compile-time string concatenation of the format literal) and rewords one log line, updating the matching Tcl test. No command reply, replication protocol byte, sta
- [valkey-io/valkey#1349](https://github.com/valkey-io/valkey/pull/1349) ([tests](tests/valkey/1349-469d41fb3/)): Avoid double close on repl_transfer_fd (#1349): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1347](https://github.com/valkey-io/valkey/pull/1347) ([tests](tests/valkey/1347-2d48a39c2/)): Save open's errno when opening temp rdb fails to prevent it from being modified (#1347): declined: The change only saves errno across a sleep() in the replica's temp-RDB open retry loop so the warning log prints the correct strerror on platforms (macOS) where sleep clobbers errno. Control flow, replies, replication behavior and data handling are unchanged; the only observable difference
- [valkey-io/valkey#1342](https://github.com/valkey-io/valkey/pull/1342) ([tests](tests/valkey/1342-cf1a1e093/)): Optimize sdscatrepr by batch processing printable characters (#1342): 1 of 2 test(s) reached the change, coverage 68%
- [valkey-io/valkey#1260](https://github.com/valkey-io/valkey/pull/1260) ([tests](tests/valkey/1260-c4920bca4/)): Integrating fast_float to optionally replace strtod (#1260): 1 of 10 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1334](https://github.com/valkey-io/valkey/pull/1334) ([tests](tests/valkey/1334-653d5f7fe/)): Support empty callback on function and free temp function in async way (#1334): 0 of 6 test(s) reached the change, coverage 95%; crash, fixed by #1334 (recorded); crash, introduced and since fixed (recorded)
- [valkey-io/valkey#1276](https://github.com/valkey-io/valkey/pull/1276) ([tests](tests/valkey/1276-33f42d7fb/)): CMake fixes + README update (#1276): declined: The change modifies only CMake build scripts (CMakeLists.txt, cmake/Modules/*, deps/*/CMakeLists.txt) and the README. It alters how the binaries are compiled and linked (sanitizer flag plumbing, tcmalloc linkage via pkg-config, jemalloc build error handling, Lua mkstemp detection) and docu
- [valkey-io/valkey#1249](https://github.com/valkey-io/valkey/pull/1249) ([tests](tests/valkey/1249-9851006d6/)): Add short client info log to CLUSTER MEET / FORGET / RESET commands (#1249): 0 of 6 test(s) reached the change, coverage 94%
- [valkey-io/valkey#1295](https://github.com/valkey-io/valkey/pull/1295) ([tests](tests/valkey/1295-b9d224097/)): Brocast a PONG to all node in cluster when role changed (#1295): 1 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1340](https://github.com/valkey-io/valkey/pull/1340) ([tests](tests/valkey/1340-979f4c1ce/)): Add cmake-build-debug and cmake-build-release to gitignore (#1340): declined: The change only adds cmake-build-debug/ and cmake-build-release/ to .gitignore. It is a repository housekeeping/build-tooling change with no effect on any Valkey binary, command, reply, replication, persistence, or tool behavior, so there is nothing a runtime scenario could exercise.
- [valkey-io/valkey#971](https://github.com/valkey-io/valkey/pull/971) ([tests](tests/valkey/971-377ed22c9/)): [feat] add Ubuntu 24.04 Noble package support (#971): declined: The change edits only utils/releasetools/build-config.json, adding Ubuntu 24.04 (noble) x86_64 and arm64 deb targets to the release packaging matrix. It is a build/CI packaging configuration change with no effect on server behaviour, commands, replication, persistence, or any tool the fuzz
- [valkey-io/valkey#1205](https://github.com/valkey-io/valkey/pull/1205) ([tests](tests/valkey/1205-18d1eb5a8/)): Remove redundant dict_index calculations (#1205): declined: This is a pure performance refactor with no behavioral change. In both lookupKey and keysCommand the change only removes redundant recomputations of a key's dict index (hash slot). getKVStoreIndexForKey is a deterministic function of the key, so computing it once and reusing it yields the 
- [valkey-io/valkey#1253](https://github.com/valkey-io/valkey/pull/1253) ([tests](tests/valkey/1253-43b502616/)): Fix argument types of formatting functions (#1253): declined: The change only corrects the argument types passed to sdscatfmt in representSlotInfo (slot ranges in CLUSTER NODES, values always 0-16383) and in valkey-cli's clusterManagerNodeInfo (a small non-negative int replica count). In both cases the values fit in the low 32 bits that were already 
- [valkey-io/valkey#1327](https://github.com/valkey-io/valkey/pull/1327) ([tests](tests/valkey/1327-50aae13b0/)): Skip reclaim file page cache test in valgrind (#1327): declined: The change is confined to the unit-test harness (src/unit/test_main.c, test_help.h, test_util.c, README): it adds a --valgrind flag and uses it to skip the reclaimFilePageCache unit test under valgrind. No server runtime, command, replication, persistence or tool behaviour changes, so no f
- [valkey-io/valkey#1274](https://github.com/valkey-io/valkey/pull/1274) ([tests](tests/valkey/1274-c4be326c3/)): Make manual failover reset the on-going election to promote failover (#1274): 2 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1266](https://github.com/valkey-io/valkey/pull/1266) ([tests](tests/valkey/1266-b56eed247/)): Remove valkey specific changes in jemalloc source code (#1266): 0 of 7 test(s) reached the change, coverage 100%

### valkey-io/valkey-bloom

[2026-11-11T00:02:57+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42928163810): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-10T23:46:58+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42926071978): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-10T23:47:28+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42926133401): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 773 of 1209 tested (64%)
- Changed-line coverage: median 100% of 737 merge(s) measured, 58 under 50%
- Recall: 204 parents flagged of 324 fix merges
- Claude: $1018.48 in the last 24 hours, $8815.87 in the last 7 days, $8815.87 in all
- Issues filed: 67
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

- [valkey-io/valkey#1948](https://github.com/valkey-io/valkey/pull/1948): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#1535](https://github.com/valkey-io/valkey/pull/1535): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
