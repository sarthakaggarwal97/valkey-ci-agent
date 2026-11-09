# Fuzzer walk

Updated 2026-11-09T14:09+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 1210 | 1297 | #1566 (2025-06-10) | 269 | 172.9 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 669 | 0 | f8106e72e984 (2024-11-27) | 60 | 95.6 |

## Last runs

### valkey-io/valkey

[2026-11-09T13:36:17+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42612697002): 20 merge(s) tested, 5 without a code change.

- [valkey-io/valkey#2287](https://github.com/valkey-io/valkey/pull/2287) ([tests](tests/valkey/2287-fc7c04e4f/)): Fix missing response when AUTH is errored inside a transaction (#2287): 0 of 4 test(s) reached the change, coverage 0%
- [valkey-io/valkey#2260](https://github.com/valkey-io/valkey/pull/2260) ([tests](tests/valkey/2260-2aea99b26/)): Do not report negative passing test count (#2260): declined: The change only edits the test_report() macro in src/testhelp.h, which is compiled solely into Valkey's internal C unit-test harness to print a test summary line; it clamps the displayed passing-test count at zero and changes no server, tool, replication, persistence, or protocol behavior.
- [valkey-io/valkey#2263](https://github.com/valkey-io/valkey/pull/2263) ([tests](tests/valkey/2263-6a259d8e0/)): Properly account for checking zmalloc_used_memory() (#2263): declined: The change modifies only src/unit/test_main.c, the C unit-test runner: it adds `test_num++` so the end-of-suite zmalloc_used_memory() leak check is counted in the test totals. It affects test-harness bookkeeping only, compiles into the unit-test binary and not into valkey-server or any too
- [valkey-io/valkey#2177](https://github.com/valkey-io/valkey/pull/2177) ([tests](tests/valkey/2177-48277209b/)): Fix valkey-cli memory leak (#2177): 2 of 2 test(s) reached the change, coverage 0%; crash, introduced and since fixed (recorded); [regressed-fix, the fix of #2177 regressed](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/113)
- [valkey-io/valkey#2254](https://github.com/valkey-io/valkey/pull/2254) ([tests](tests/valkey/2254-d6507f3a0/)): Avoid freeing cluster link before printing link sender (#2254): 0 of 5 test(s) reached the change, coverage 0%
- [valkey-io/valkey#2174](https://github.com/valkey-io/valkey/pull/2174) ([tests](tests/valkey/2174-4a3e71303/)): Converge divergent shard-id persisted in nodes.conf to primary's shard id (#2174): 0 of 4 test(s) reached the change, coverage 0%, 1 skipped (required capability 'save-behavior-support' is unsupported: default: build rejects startup configuration: *** FATAL CONFIG FILE ERROR (Version 255.255.255) *** Reading the configuration file, at line 11 >>> 'cluster-config-save-behavior "sync"' Bad directive or wrong number of arguments)
- [valkey-io/valkey#2214](https://github.com/valkey-io/valkey/pull/2214) ([tests](tests/valkey/2214-502dd451d/)): Fix incorrect laddr field and correct getClientSockname declaration (#2214): declined: The change only fixes the laddr argument passed to an LTTng tracepoint (valkey_commands_trace in call()), renames tracepoint fields, corrects a function-prototype casing mismatch in server.h, and updates the trace README. None of this affects command replies, CLIENT INFO/LIST output, repli
- [valkey-io/valkey#2216](https://github.com/valkey-io/valkey/pull/2216) ([tests](tests/valkey/2216-3a01c7f7e/)): Fix unixsocket too long cause addr / laddr being truncated in CLIENT INFO / LIST and logging (#2216): 3 of 3 test(s) reached the change, coverage 90%
- [valkey-io/valkey#2229](https://github.com/valkey-io/valkey/pull/2229) ([tests](tests/valkey/2229-04353aea3/)): Ensure empty error tables in scripts don't crash Valkey (#2229): 2 of 2 test(s) reached the change; crash, fixed by #2229 (recorded)
- [valkey-io/valkey#1292](https://github.com/valkey-io/valkey/pull/1292) ([tests](tests/valkey/1292-a1f4cd6f2/)): Improve "SENTINEL FAILOVER" by using the "FAILOVER" command (#1292): declined: The change is confined to Sentinel mode: new COORDINATED option and failover state machine in src/sentinel.c, plus SENTINEL FAILOVER command metadata. The fuzzer can only deploy standalone/cluster data nodes and private servers started as ordinary valkey-server processes; sentinel mode req
- [valkey-io/valkey#2223](https://github.com/valkey-io/valkey/pull/2223) ([tests](tests/valkey/2223-62376a315/)): Add non-fatal unit test assertions (#2223): declined: The change adds non-fatal assertion macros to the C unit-test framework (src/unit/test_help.h, test_main.c) and documents them in src/unit/README.md. It modifies only test-harness code compiled into the unit-test binary, not the valkey-server or any tool shipped in a build; no command, rep
- [valkey-io/valkey#2225](https://github.com/valkey-io/valkey/pull/2225) ([tests](tests/valkey/2225-a05d1a6ec/)): Fix the incorrect stdout redirect logic, use dup2 to redirect stdout  (#2225): 3 of 3 test(s) reached the change, coverage 100%; crash, predates #2225, since fixed (recorded)
- [valkey-io/valkey#2226](https://github.com/valkey-io/valkey/pull/2226) ([tests](tests/valkey/2226-ceb09b0a9/)): Reset failover failed primary rank when doing a manual failover (#2226): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2160](https://github.com/valkey-io/valkey/pull/2160) ([tests](tests/valkey/2160-3b25c4d6b/)): Optimize scan/sscan/hscan/zscan commands by replacing list with vector (#2160): 0 of 4 test(s) reached the change, coverage 98%
- [valkey-io/valkey#2206](https://github.com/valkey-io/valkey/pull/2206) ([tests](tests/valkey/2206-81e00517c/)): Prevent getNodeByQuery from leaking DB changes into client (#2206): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1743](https://github.com/valkey-io/valkey/pull/1743) ([tests](tests/valkey/1743-e71f03ca0/)): Support for RDB analysis reports (#1743): 3 of 3 test(s) reached the change, coverage 88%
- [valkey-io/valkey#2201](https://github.com/valkey-io/valkey/pull/2201) ([tests](tests/valkey/2201-95b6a32f5/)): test: fix UBSan warning due to misaligned memory access in unit test_addRepliesWithOffloadsToList (#2201): declined: The change edits only the unit test file src/unit/test_networking.c, replacing misaligned robj** dereferences with memcpy to fix a UBSan warning inside the tests themselves. No server source code, command behaviour, reply format, replication, persistence, or tool code changes, so no fuzzer
- [valkey-io/valkey#2198](https://github.com/valkey-io/valkey/pull/2198) ([tests](tests/valkey/2198-5b368352b/)): Remove unused repl_offset_time field in clusterNode (#2198): declined: The change deletes the clusterNode.repl_offset_time field, its zero-initialization in createClusterNode, and its single assignment in clusterProcessPacket. The field was only ever written, never read — a grep of the post-change tree shows no remaining references, and the pre-change code ha
- [valkey-io/valkey#2188](https://github.com/valkey-io/valkey/pull/2188) ([tests](tests/valkey/2188-7bc34cf87/)): fix: use replica's last selected db for dual-channel sync (#2188): 2 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2193](https://github.com/valkey-io/valkey/pull/2193) ([tests](tests/valkey/2193-c41ffc340/)): valkey-check-rdb: Fix truncated long aux fields (#2193): declined: no proposed test validated

### valkey-io/valkey-bloom

[2026-11-09T14:09:33+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42631817276): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-09T13:41:42+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42627644366): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-09T13:49:16+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42628701496): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 665 of 1001 tested (66%)
- Changed-line coverage: median 100% of 555 merge(s) measured, 45 under 50%
- Recall: 186 parents flagged of 283 fix merges
- Claude: $1039.18 in the last 24 hours, $7226.23 in the last 7 days, $7226.23 in all
- Issues filed: 64
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
