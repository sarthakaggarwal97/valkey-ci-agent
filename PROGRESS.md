# Fuzzer walk

Updated 2026-11-07T23:55+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 812 | 1695 | #3076 (2026-01-20) | 356 | 116.0 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 505 | 164 | #308 (2025-08-15) | 171 | 72.1 |

## Last runs

### valkey-io/valkey

[2026-11-07T23:06:41+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42415598670): 20 merge(s) tested, 7 without a code change.

- [valkey-io/valkey#3126](https://github.com/valkey-io/valkey/pull/3126) ([tests](tests/valkey/3126-1cc268de9/)): Fix mixup of reply-larger-than and request-larger-than (#3126): 4 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3067](https://github.com/valkey-io/valkey/pull/3067) ([tests](tests/valkey/3067-8a9c3717d/)): Don't exit the process when locale-collate fails to be set from env during startup (#3067): declined: The only behaviour change is at startup: when locale-collate is at its empty default and setlocale(LC_COLLATE, "") fails, the server now continues with the default locale instead of exit(1). That branch is taken only when the valkey-server process's own environment variables (LC_ALL/LC_COL
- [valkey-io/valkey#2867](https://github.com/valkey-io/valkey/pull/2867) ([tests](tests/valkey/2867-920bf128b/)): Optimize skiplist query efficiency by embedding the skiplist header (#2867): 0 of 7 test(s) reached the change, coverage 93%
- [valkey-io/valkey#2928](https://github.com/valkey-io/valkey/pull/2928) ([tests](tests/valkey/2928-1fd6d7156/)): decouple lru/lfu from server.h (#2928): 2 of 5 test(s) reached the change, coverage 86%, 1 skipped (required capability 'build' is unsupported: default: build rejects startup configuration: *** FATAL CONFIG FILE ERROR (Version 255.255.255) *** Reading the configuration file, at line 11 >>> 'forkless)
- [valkey-io/valkey#2953](https://github.com/valkey-io/valkey/pull/2953) ([tests](tests/valkey/2953-f5d7dc8d0/)): Fix potential memory leaks when a writable replica is promoted to primary after direct writes of keys with expiration (#2953): 1 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2999](https://github.com/valkey-io/valkey/pull/2999) ([tests](tests/valkey/2999-eb3f465e5/)): Fail fast on invalid certificates at TLS config load (#2999): declined: The change only adds certificate-validity checks in src/tls.c's createSSLContext, reached when a TLS-enabled build loads a TLS config (at startup or on CONFIG SET of a tls-* option). Exercising the new branch needs a BUILD_TLS=yes binary, an active TLS listener, and deliberately expired/no
- [valkey-io/valkey#3103](https://github.com/valkey-io/valkey/pull/3103) ([tests](tests/valkey/3103-0422fc66c/)): Enable HW clock by default (#3103): 1 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3117](https://github.com/valkey-io/valkey/pull/3117) ([tests](tests/valkey/3117-928e91261/)): remove unneeded include of expire.h (#3117): declined: The change only removes an unneeded #include "expire.h" from src/t_hash.c. No executable code, reply, data format, or configuration changes; any breakage would be a compile failure caught by the build, not a runtime behaviour a fuzzer scenario could reach.
- [valkey-io/valkey#2463](https://github.com/valkey-io/valkey/pull/2463) ([tests](tests/valkey/2463-4ad7628d0/)): Instrumenting used active time metrics for I/O threads (#2463): 2 of 4 test(s) reached the change, coverage 93%
- [valkey-io/valkey#3086](https://github.com/valkey-io/valkey/pull/3086) ([tests](tests/valkey/3086-985cf836a/)): Perf: Track net bytes only if commandlog-request-larger-than != -1 (#3086): 3 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3060](https://github.com/valkey-io/valkey/pull/3060) ([tests](tests/valkey/3060-94edae4b8/)): Fix how hash is handling overriding of expired fields overwrite (#3060): 4 of 11 test(s) reached the change, coverage 83%
- [valkey-io/valkey#3107](https://github.com/valkey-io/valkey/pull/3107) ([tests](tests/valkey/3107-e7d004be0/)): Comment cleanup around entry.c file (#3107): declined: The change is comment and whitespace cleanup in entry.c. The only two code edits are behavior-neutral: (1) entryGetExpiryRef's debugServerAssert was upgraded to serverAssert, but all three callers (entryGetExpiry, entrySetExpiry, and the guarded call in entryUpdateAsStringRef) already esta
- [valkey-io/valkey#3015](https://github.com/valkey-io/valkey/pull/3015) ([tests](tests/valkey/3015-278607b74/)): makefile formatting: split lists onto multiple lines for better readability and easier merging (#3015): declined: The change is purely a Makefile formatting refactor: the ENGINE_SERVER_OBJ, ENGINE_TRACE_OBJ, ENGINE_CLI_OBJ, ENGINE_BENCHMARK_OBJ and Lua-module object lists are split onto multiple lines and sorted alphabetically, with the identical set of object files before and after. No source file, c
- [valkey-io/valkey#3073](https://github.com/valkey-io/valkey/pull/3073) ([tests](tests/valkey/3073-d8315cfcf/)): Improve performance during rehashing (#3073): 0 of 6 test(s) reached the change, coverage 100%, 1 skipped (required capability 'build' is unsupported: default: build rejects startup configuration: *** FATAL CONFIG FILE ERROR (Version 255.255.255) *** Reading the configuration file, at line 11 >>> 'forkless)
- [valkey-io/valkey#3098](https://github.com/valkey-io/valkey/pull/3098) ([tests](tests/valkey/3098-be0a572c8/)): Fix minor inconsistency in connFormatAddr size argument (#3098): declined: The change replaces sizeof(addr) with sizeof(laddr) in a connFormatAddr call, but both buffers are declared with the same size (char [CONN_ADDR_STR_LEN]), so the passed value is the identical compile-time constant. It is a pure code-consistency refactor with no behaviour change: no reply, 
- [valkey-io/valkey#2742](https://github.com/valkey-io/valkey/pull/2742) ([tests](tests/valkey/2742-231a9e731/)): Fix XREAD returning error on empty stream with + ID (#2742): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3088](https://github.com/valkey-io/valkey/pull/3088) ([tests](tests/valkey/3088-2e2eb418f/)): Fix the memory leak in the VM_GetCommandKeysWithFlags function (#3088): 0 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3022](https://github.com/valkey-io/valkey/pull/3022) ([tests](tests/valkey/3022-4b5a48a36/)): Fix HRANDFIELD to return null response when no field could be found (#3022): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3020](https://github.com/valkey-io/valkey/pull/3020) ([tests](tests/valkey/3020-3c092caa0/)): Automatic TLS reload (#3020): 2 of 7 test(s) reached the change, coverage 0%
- [valkey-io/valkey#3081](https://github.com/valkey-io/valkey/pull/3081) ([tests](tests/valkey/3081-1084e671c/)): improve format for hashtableDump (#3081): declined: The change only reworks printf formatting inside hashtableDump, a debug dump helper in src/hashtable.c. The function is not called from any server code path — its only reference besides the definition is a forward declaration in the unit test file src/unit/test_hashtable.c — so no command 

### valkey-io/valkey-bloom

[2026-11-07T23:55:47+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42427394963): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-07T23:38:40+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42426138849): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-07T21:51:33+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42406092025): 20 merge(s) tested, 4 without a code change.

- [valkey-io/valkey-search#362](https://github.com/valkey-io/valkey-search/pull/362) ([tests](tests/valkey-search/362-cac7af75e/)): Add configurable thread pool fairness mechanism with comprehensive testing (#362): 5 of 5 test(s) reached the change; crash, predates #362, since fixed (recorded)
- [valkey-io/valkey-search#374](https://github.com/valkey-io/valkey-search/pull/374) ([tests](tests/valkey-search/374-d71d44b9b/)): Increase M minimum limit to 2 (#374): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search#358](https://github.com/valkey-io/valkey-search/pull/358) ([tests](tests/valkey-search/358-0d8a6dc02/)): Implement Controlled Variables and TEST_COUNTER macros (#358): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#347](https://github.com/valkey-io/valkey-search/pull/347) ([tests](tests/valkey-search/347-1828dfe4d/)): refactor: Replace is_local_search boolean with SearchMode enum + add eviction tests (#347): 3 of 3 test(s) reached the change; crash, predates #347, since fixed (recorded)
- [valkey-io/valkey-search#320](https://github.com/valkey-io/valkey-search/pull/320) ([tests](tests/valkey-search/320-022beaa64/)): End to end test for JSON backfill replication. Plus enhances to pytest environment. (#320): declined: no proposed test validated
- [valkey-io/valkey-search#287](https://github.com/valkey-io/valkey-search/pull/287) ([tests](tests/valkey-search/287-c04c5bab0/)): Add support to retrieving JSON values via the JSON shared API (#287): 0 of 3 test(s) reached the change
- [valkey-io/valkey-search#321](https://github.com/valkey-io/valkey-search/pull/321) ([tests](tests/valkey-search/321-35be0592c/)): Update clang tidy/format github job and apply formatting (#321): declined: The change is a CI workflow update plus a repository-wide clang-format application. Every hunk in src/ and vmsdk/ is mechanical reformatting: line re-wrapping, indentation and brace placement, include reordering within the same translation unit, comment re-flow, and namespace closing comme
- [valkey-io/valkey-search#346](https://github.com/valkey-io/valkey-search/pull/346) ([tests](tests/valkey-search/346-641bca69e/)): Implement Pause point (#346): 3 of 3 test(s) reached the change; [crash, appeared after #346](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/87)
- [valkey-io/valkey-search#312](https://github.com/valkey-io/valkey-search/pull/312) ([tests](tests/valkey-search/312-1b26dd10b/)): Update Vector HNSW output field format (#312): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#339](https://github.com/valkey-io/valkey-search/pull/339) ([tests](tests/valkey-search/339-a5d63ae10/)): Introduced new "debug-mode" configuration entry. (#339): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#301](https://github.com/valkey-io/valkey-search/pull/301) ([tests](tests/valkey-search/301-459889b3d/)): Add a `max-worker-suspension` configuration for resuming workers after fork (#301): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search#329](https://github.com/valkey-io/valkey-search/pull/329) ([tests](tests/valkey-search/329-e78b45d1e/)): Expose global symbols so that crash dump backtraces are useful (#329): 0 of 3 test(s) reached the change
- [valkey-io/valkey-search#330](https://github.com/valkey-io/valkey-search/pull/330) ([tests](tests/valkey-search/330-446d9f855/)): Enable ASan runs for OSS integ tests (#330): declined: The change modifies only build/CI scripts (build.sh, integration/run.sh, testing/integration/run.sh, scripts/common.rc) and Python integration-test scaffolding (test_info.py skip marker, LOGS_DIR handling in valkey_search_test_case.py) to enable ASan for OSS integration tests. No module so
- [valkey-io/valkey-search#325](https://github.com/valkey-io/valkey-search/pull/325) ([tests](tests/valkey-search/325-e67f6c148/)): change encoding version to version (#325): declined: The change renames InfoIndexPartitionResponse field 16 from encoding_version to version and feeds it from the per-index monotonic entry.version() instead of the constant entry.encoding_version() (always 1). This value is used only for the internal cross-node consistency check in the FT.INF
- [valkey-io/valkey-search#323](https://github.com/valkey-io/valkey-search/pull/323) ([tests](tests/valkey-search/323-61499555d/)): Update 3 fields returned type of FT.INFO (#323): 2 of 2 test(s) reached the change; crash, predates #323, since fixed (recorded)
- [valkey-io/valkey-search#311](https://github.com/valkey-io/valkey-search/pull/311) ([tests](tests/valkey-search/311-b32447762/)): feat: add integration tests for expired hashes and FT.INFO parser (#311): declined: The change only adds Python integration tests and a test-suite FT.INFO parser (ft_info_parser.py, test_expired.py, refactors of test_cancel.py/test_flushall.py/indexes.py) plus a CMAKE_POLICY_VERSION_MINIMUM export in build.sh. No module source code under src/ changed, so the module's runt
- [valkey-io/valkey-search#309](https://github.com/valkey-io/valkey-search/pull/309) ([tests](tests/valkey-search/309-607a7982d/)): Fix crash due to Valkey Timeout before Cancellation timeout (#309): 3 of 3 test(s) reached the change; crash, predates #309, since fixed (recorded)
- [valkey-io/valkey-search#314](https://github.com/valkey-io/valkey-search/pull/314) ([tests](tests/valkey-search/314-8fc348ffa/)): Additionale metrics of info search active_indexes active_indexes_running_queries total_active_write_threads total_indexing_time active_indexes_indexing (#314): 2 of 2 test(s) reached the change; crash, predates #314, since fixed (recorded)
- [valkey-io/valkey-search#153](https://github.com/valkey-io/valkey-search/pull/153) ([tests](tests/valkey-search/153-830d1a19c/)): feat: memory scope & memory usage for string interning store (#153): 4 of 4 test(s) reached the change
- [valkey-io/valkey-search@d8e943f6a260](https://github.com/valkey-io/valkey-search/commit/d8e943f6a2606c876284e9377504b74da0a7538d) ([tests](tests/valkey-search/d8e943f6a/)): Revert "Update 3 fields returned type of FT.INFO (#319)": 2 of 2 test(s) reached the change; crash, predates d8e943f6a260, since fixed (recorded)

## Totals

- Merges whose tests reached the change: 510 of 744 tested (69%)
- Changed-line coverage: median 100% of 363 merge(s) measured, 23 under 50%
- Recall: 146 parents flagged of 217 fix merges
- Claude: $1834.38 in the last 24 hours, $5318.14 in the last 7 days, $5318.14 in all
- Issues filed: 53
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/100
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/101
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/102
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

- [valkey-io/valkey#3800](https://github.com/valkey-io/valkey/pull/3800): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#3613](https://github.com/valkey-io/valkey/pull/3613): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#3498](https://github.com/valkey-io/valkey/pull/3498): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
