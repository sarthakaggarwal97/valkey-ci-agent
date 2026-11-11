# Fuzzer walk

Updated 2026-11-11T13:54+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 1799 | 724 | #877 (2024-09-02) | 293 | 257.0 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 64 | 0 | c1423d7e4b38 (2024-11-25) | 1 | 9.1 |
| valkey-io/valkey-search | 672 | 0 | f8106e72e984 (2024-11-27) | 0 | 96.0 |

## Last runs

### valkey-io/valkey

[2026-11-11T13:33:08+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43010363615): 20 merge(s) tested, 0 without a code change.

- [valkey-io/valkey#5349](https://github.com/valkey-io/valkey/pull/5349) ([tests](tests/valkey/5349-d5bf376a3/)): Treat a torn annotation at the end of an AOF file as a short read (#5349): 1 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1115](https://github.com/valkey-io/valkey/pull/1115) ([tests](tests/valkey/1115-150c197bd/)): Apply CVE patches for CVE-2024-31449, CVE-2024-31227, CVE-2024-31228 (#1115): 2 of 3 test(s) reached the change, coverage 100%; crash, fixed by #1115 (recorded)
- [valkey-io/valkey#1001](https://github.com/valkey-io/valkey/pull/1001) ([tests](tests/valkey/1001-4033c99ef/)): Fix module RdbLoad wrongly disable the AOF (#1001): 1 of 1 test(s) reached the change, coverage 100%
- [valkey-io/valkey#989](https://github.com/valkey-io/valkey/pull/989) ([tests](tests/valkey/989-1b2416845/)): Dual Channel Replication - Verify Replica Local Buffer Limit Configuration (#989): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#996](https://github.com/valkey-io/valkey/pull/996) ([tests](tests/valkey/996-58fe9c013/)): Use hashtable as the default type of temp set object during sunion/sdiff (#996): 2 of 2 test(s) reached the change, coverage 100%; [regressed-fix, the fix of #996 regressed](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/126)
- [valkey-io/valkey#1011](https://github.com/valkey-io/valkey/pull/1011) ([tests](tests/valkey/1011-9f0c80187/)): Fix crash in async IO threads with TLS (#1011): 0 of 6 test(s) reached the change, coverage 100%
- [valkey-io/valkey#778](https://github.com/valkey-io/valkey/pull/778) ([tests](tests/valkey/778-affbea5dc/)): For MEETs, save the extensions support flag immediately during MEET processing (#778): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1002](https://github.com/valkey-io/valkey/pull/1002) ([tests](tests/valkey/1002-50c1fe59f/)): Add missing moduleapi getchannels test and fix tests (#1002): declined: The change modifies only test infrastructure: it registers the existing getchannels moduleapi test in the runtest-moduleapi runner script and updates the expected ACL DRYRUN error-message strings in tests/unit/moduleapi/getchannels.tcl to match the server's current message format ('User <n
- [valkey-io/valkey#1004](https://github.com/valkey-io/valkey/pull/1004) ([tests](tests/valkey/1004-f504cf233/)): add assertion for kvstore's dictType (#1004): declined: The change only adds assertions in kvstoreCreate that the dictType's rehashingStarted, rehashingCompleted and dictMetadataBytes callbacks are the kvstore-specific ones. Every existing caller (kvstoreKeysDictType, kvstoreExpiresDictType, kvstoreChannelDictType in server.c) already sets exac
- [valkey-io/valkey#946](https://github.com/valkey-io/valkey/pull/946) ([tests](tests/valkey/946-20d583f77/)): Migrate dict.c unit tests to new framework (#946): declined: The change only migrates dict.c's unit tests from the legacy SERVER_TEST in-binary test runner to the new standalone unit-test framework (new src/unit/test_dict.c, removal of the #ifdef SERVER_TEST block in dict.c, its prototype in dict.h, and the 'dict' entry in server.c's serverTests tab
- [valkey-io/valkey#949](https://github.com/valkey-io/valkey/pull/949) ([tests](tests/valkey/949-14016d2df/)): Migrate listpack.c unit tests to new framework  (#949): declined: The change only migrates listpack unit tests to the new test framework: it deletes the #ifdef SERVER_TEST test block from listpack.c (compiled only into test builds, never the shipped server), removes the test declaration from listpack.h and the test-dispatch entry from server.c's serverTe
- [valkey-io/valkey#875](https://github.com/valkey-io/valkey/pull/875) ([tests](tests/valkey/875-c642cf013/)): Add client info to SHUTDOWN / CLUSTER FAILOVER logs (#875): 0 of 8 test(s) reached the change, coverage 94%; [crash, predates #875](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/127)
- [valkey-io/valkey#886](https://github.com/valkey-io/valkey/pull/886) ([tests](tests/valkey/886-647852659/)): Fix aof base suffix when modifying aof-use-rdb-preamble during rewrite (#886): 1 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#965](https://github.com/valkey-io/valkey/pull/965) ([tests](tests/valkey/965-9b51949ab/)): Fix missing replication link re-connection when primary's IP/port is updated in `clusterProcessGossipSection` (#965): 0 of 3 test(s) reached the change, coverage 0%
- [valkey-io/valkey#993](https://github.com/valkey-io/valkey/pull/993) ([tests](tests/valkey/993-9033734b6/)): Add newline to argv in crash report when doing redact (#993): declined: The change only fixes log formatting in the crash report: it removes a trailing space and adds a missing newline to the serverLog lines printed for redacted argv entries in _serverAssertPrintClientInfo and logCurrentClient (src/debug.c). These functions run only inside the assertion/crash 
- [valkey-io/valkey#988](https://github.com/valkey-io/valkey/pull/988) ([tests](tests/valkey/988-2d1eca577/)): Add SLOT-STATS under CLUSTER HELP string. (#988): 0 of 2 test(s) reached the change
- [valkey-io/valkey#983](https://github.com/valkey-io/valkey/pull/983) ([tests](tests/valkey/983-ea58fbf40/)): Rewrite lazyfree docs in valkey.conf to reflect that lazy is now default (#983): declined: The change edits only comment text in valkey.conf: it rewrites the LAZY FREEING section's documentation to say lazy freeing is the default since Valkey 8.0. Every configuration directive in the section keeps its existing name and value (all remain 'yes'), and no C source, parsing code, or 
- [valkey-io/valkey#979](https://github.com/valkey-io/valkey/pull/979) ([tests](tests/valkey/979-f143ffd2a/)): Fix typo in valkey-cli.c (#979): declined: The change only corrects a typo in a log message string ("replicsa" -> "replicas") printed by valkey-cli's clusterManagerOptimizeAntiAffinity warning. No control flow, command behavior, reply, exit status, or data path changes; it is a cosmetic text fix with no behavior to regress, so no s
- [valkey-io/valkey#749](https://github.com/valkey-io/valkey/pull/749) ([tests](tests/valkey/749-981f977ab/)): Improve type safety and refactor dict entry handling (#749): 0 of 9 test(s) reached the change, coverage 81%
- [valkey-io/valkey#982](https://github.com/valkey-io/valkey/pull/982) ([tests](tests/valkey/982-3e14516d8/)): Initialize all the fields for the test kvstore (#982): declined: The change modifies only src/unit/test_kvstore.c, a unit-test fixture: it initializes the kvstore rehashing and metadata-size callbacks in the test-only dictType so the kvstore unit tests exercise the real callback paths. No shipped server code, command behaviour, replication, persistence,

### valkey-io/valkey-bloom

[2026-11-11T13:35:21+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43030401137): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-11T13:53:45+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43033200476): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-11T13:54:54+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43033353033): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 804 of 1278 tested (63%)
- Changed-line coverage: median 100% of 798 merge(s) measured, 64 under 50%
- Recall: 217 parents flagged of 342 fix merges
- Claude: $950.56 in the last 24 hours, $9311.36 in the last 7 days, $9311.36 in all
- Issues filed: 71
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
