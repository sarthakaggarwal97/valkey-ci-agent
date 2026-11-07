# Fuzzer walk

Updated 2026-11-07T21:51+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 785 | 1722 | #3126 (2026-01-29) | 349 | 112.1 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 505 | 164 | #308 (2025-08-15) | 196 | 72.1 |

## Last runs

### valkey-io/valkey

[2026-11-07T21:22:56+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42407462314): 20 merge(s) tested, 8 without a code change.

- [valkey-io/valkey#3135](https://github.com/valkey-io/valkey/pull/3135) ([tests](tests/valkey/3135-751a09964/)): Add remaining_repl_size field in CLUSTER MIGRATESLOTS output (#3135): 3 of 3 test(s) reached the change, coverage 100%; [check, introduced by #3135](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/100); [check, introduced by #3135](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/101)
- [valkey-io/valkey#2868](https://github.com/valkey-io/valkey/pull/2868) ([tests](tests/valkey/2868-87caeb764/)): Add more client info flags to module API (#2868): 3 of 3 test(s) reached the change, coverage 100%; check, not reproduced (recorded); check, introduced and since fixed (recorded); check, not reproduced (recorded); check, introduced and since fixed (recorded); [crash, appeared after #2868](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/102); crash, predates #2868, since fixed (recorded)
- [valkey-io/valkey#3167](https://github.com/valkey-io/valkey/pull/3167) ([tests](tests/valkey/3167-ca34cc0b8/)): Make TLS disable/enable INFO checks deterministic for module mode (#3167): declined: The only behavioral change is in TLS module mode (BUILD_TLS=module): the server's non-OpenSSL tlsResetCertInfo stub no longer wipes the INFO TLS cert serial/expiry fields that the loaded valkey-tls module populated, when a TLS config is set while TLS stays enabled. The fuzzer cannot reach 
- [valkey-io/valkey#3173](https://github.com/valkey-io/valkey/pull/3173) ([tests](tests/valkey/3173-e84852dca/)): Avoid crash in genClusterInfoString when myself is uninitialized (#3173): declined: The fix adds a NULL guard in genClusterInfoString so that nodeEpoch is not called on a NULL `myself`. `myself` is only ever NULL in one window: during clusterInit() itself, if clusterLoadConfig() hits a corrupted cluster-config-file and calls serverPanic (cluster_legacy.c:1445/1012) before
- [valkey-io/valkey#3159](https://github.com/valkey-io/valkey/pull/3159) ([tests](tests/valkey/3159-55b846064/)): Minor cleanup in memtoull to also check ERANGE when calling strtoull (#3159): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3174](https://github.com/valkey-io/valkey/pull/3174) ([tests](tests/valkey/3174-980ef5782/)): make test_empty_buckets_rehashing deterministic across hash seeds (#3174): declined: The change only edits the unit test src/unit/test_hashtable.c to make test_empty_buckets_rehashing deterministic across randomized hash seeds (choosing which key to keep by its bucket index and adding fresh keys instead of reused ones). No Valkey server code, command, data structure, repli
- [valkey-io/valkey#3155](https://github.com/valkey-io/valkey/pull/3155) ([tests](tests/valkey/3155-666472c76/)): Fix arr bound err in test-sanitizer-undefined (gcc) (#3155): 0 of 3 test(s) reached the change
- [valkey-io/valkey#3150](https://github.com/valkey-io/valkey/pull/3150) ([tests](tests/valkey/3150-346c11dde/)): Rehashing more empty buckets to optimize the rehashing speed (#3150): 1 of 3 test(s) reached the change, coverage 80%
- [valkey-io/valkey#3154](https://github.com/valkey-io/valkey/pull/3154) ([tests](tests/valkey/3154-9f374e158/)): Skips the internal clients from logresreq checks (#3154): declined: The change only touches reqresShouldLog() in src/logreqres.c, which is entirely inside #ifdef LOG_REQ_RES — a compile-time feature the Makefile enables only for the reply-schema validation CI tooling, and which is driven solely by the hidden, immutable req-res-logfile config that itself ex
- [valkey-io/valkey#2839](https://github.com/valkey-io/valkey/pull/2839) ([tests](tests/valkey/2839-5d7d54249/)): Add cache for COMMAND (#2839): 0 of 4 test(s) reached the change, coverage 97%
- [valkey-io/valkey#2913](https://github.com/valkey-io/valkey/pull/2913) ([tests](tests/valkey/2913-e4804d50f/)): Add server side TLS certificate expiry tracking and INFO telemetry (#2913): 2 of 2 test(s) reached the change, coverage 78%
- [valkey-io/valkey#3144](https://github.com/valkey-io/valkey/pull/3144) ([tests](tests/valkey/3144-8ed270f44/)): Optimize SREM/ZREM/HDEL to pause auto shrink when deleting multiple items (#3144): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3141](https://github.com/valkey-io/valkey/pull/3141) ([tests](tests/valkey/3141-79c8948ed/)): Optimize rehash completion check for empty hash table (#3141): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3142](https://github.com/valkey-io/valkey/pull/3142) ([tests](tests/valkey/3142-cfbe0937f/)): Extend hashtableGetStatsMsg to always show empty table and rehash index (#3142): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3116](https://github.com/valkey-io/valkey/pull/3116) ([tests](tests/valkey/3116-eabfe235d/)): Check expiration time first before lookup the key in GETEX command (#3116): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3137](https://github.com/valkey-io/valkey/pull/3137) ([tests](tests/valkey/3137-9e0303bc5/)): Revert #3088 (#3137): 0 of 1 test(s) reached the change
- [valkey-io/valkey#2948](https://github.com/valkey-io/valkey/pull/2948) ([tests](tests/valkey/2948-851eaafe6/)): Free replica local buffer in async way in dual channel replication (#2948): 1 of 8 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3128](https://github.com/valkey-io/valkey/pull/3128) ([tests](tests/valkey/3128-73838a0dd/)): Remove empty list dead code condition in lmoveGenericCommand code (#3128): declined: This is a pure dead-code removal refactor with no observable behavior change. The deleted branch in lmoveGenericCommand handled an existing key holding an empty (zero-length) list, but no client-reachable path can produce such a key: every list loader in rdb.c routes zero-element lists to 
- [valkey-io/valkey#3123](https://github.com/valkey-io/valkey/pull/3123) ([tests](tests/valkey/3123-b8650a5a3/)): Fix Lua Module Build For MacOS Compatibility (#3123): declined: The change is purely a CMake build-system fix for macOS: it adds -fPIC to the fpconv static library, selects libvalkeylua.dylib as the module name on Apple, and bundles sha1.c, rand.c and fpconv into the valkeylua shared library so it links on macOS. No C source, command, reply, replicatio
- [valkey-io/valkey#3001](https://github.com/valkey-io/valkey/pull/3001) ([tests](tests/valkey/3001-fd1438063/)): HSETEX - Always issue keyspace notifications after validation (#3001): 7 of 7 test(s) reached the change, coverage 100%

### valkey-io/valkey-bloom

[2026-11-07T21:37:37+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42416441043): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-07T21:36:06+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42416293159): 0 merge(s) tested, 0 without a code change.


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

- Merges whose tests reached the change: 500 of 731 tested (68%)
- Changed-line coverage: median 100% of 350 merge(s) measured, 22 under 50%
- Recall: 141 parents flagged of 211 fix merges
- Claude: $1930.71 in the last 24 hours, $5223.65 in the last 7 days, $5223.65 in all
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
