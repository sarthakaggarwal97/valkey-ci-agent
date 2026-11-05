# Fuzzer walk

Updated 2026-11-05T23:28+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 233 | 2259 | #4506 (2026-09-16) | 233 | 33.3 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 107 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 60 | 8.9 |
| valkey-io/valkey-search | 125 | 543 | #946 (2026-08-19) | 125 | 17.9 |

## Last runs

### valkey-io/valkey

[2026-11-05T22:45:05+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42113690922): 20 merge(s) tested, 9 without a code change.

- [valkey-io/valkey#3645](https://github.com/valkey-io/valkey/pull/3645) ([tests](tests/valkey/3645-f4cbd1c5e/)): fix: Reject module writes during client pause to prevent crash (#3645): 1 of 7 test(s) reached the change, coverage 25%; crash, fixed by #3645 (recorded); [crash, appeared after #3645](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/70); crash, introduced and since fixed (recorded)
- [valkey-io/valkey#4777](https://github.com/valkey-io/valkey/pull/4777) ([tests](tests/valkey/4777-06050cb1f/)): Fix geohashGetLatDistance() when latitudes are equal (#4777): 2 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3491](https://github.com/valkey-io/valkey/pull/3491) ([tests](tests/valkey/3491-c79f17d25/)): zset: add in-place fast path for score updates in listpack encoding (#3491): 0 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3157](https://github.com/valkey-io/valkey/pull/3157) ([tests](tests/valkey/3157-60f3944c1/)): Increase valkey-benchmark max latency bucket to 60 seconds (#3157): 2 of 2 test(s) reached the change
- [valkey-io/valkey#4745](https://github.com/valkey-io/valkey/pull/4745) ([tests](tests/valkey/4745-019ec0bc2/)): Remove duplicate pointer-key hashtable callbacks (#4745): declined: Pure refactor with no observable behaviour change. In bgiteration.c the removed pointerHash was byte-for-byte Thomas Wang's 64-bit mix, which is exactly what the new shared hashtablePointerHash computes (wangHash64), and the removed pointerCompare equals the hashtable's NULL-callback defau
- [valkey-io/valkey#4750](https://github.com/valkey-io/valkey/pull/4750) ([tests](tests/valkey/4750-77b00b2dd/)): bgiteration: fix barrier item count for swap/flush during termination (#4750): declined: The fix removes a double-decrement of bgIterator.barrier_items that only occurs when queued SWAPDB/FLUSHDB barrier items are drained during iterator termination. Those barrier items are queued exclusively for non-consistent background iterators: handleSwapdb (bgiteration.c:1801) queues SWA
- [valkey-io/valkey#4752](https://github.com/valkey-io/valkey/pull/4752) ([tests](tests/valkey/4752-504562458/)): Remove duplicate clientsCronTcpIsClosing() call in clientsCron (#4752): declined: The change deletes a redundant second call to clientsCronTcpIsClosing() in clientsCron(); the same function is still called first in the per-client check chain (server.c:1301), and none of the checks between the two call sites (query/output buffer resize, expensive-client tracking) can cha
- [valkey-io/valkey#3798](https://github.com/valkey-io/valkey/pull/3798) ([tests](tests/valkey/3798-f2bcd083d/)): ZSTD Support for Streaming Compression  (#3798): 1 of 12 test(s) reached the change, coverage 90%
- [valkey-io/valkey#4739](https://github.com/valkey-io/valkey/pull/4739) ([tests](tests/valkey/4739-27c68f14f/)): Fix valkey-check-aof detection of Valkey RDB base files (#4739): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4747](https://github.com/valkey-io/valkey/pull/4747) ([tests](tests/valkey/4747-eae50c20c/)): Regenerate commands.def for DELEX (#4747): 0 of 2 test(s) reached the change
- [valkey-io/valkey#4607](https://github.com/valkey-io/valkey/pull/4607) ([tests](tests/valkey/4607-7945915f5/)): Add DELEX command with conditional predicates (#4607): 7 of 10 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4738](https://github.com/valkey-io/valkey/pull/4738) ([tests](tests/valkey/4738-11387e50c/)): Persist OPENSSL_PREFIX across builds (#4738): declined: The change is build-system only: a single line in src/Makefile that writes OPENSSL_PREFIX into .make-settings during 'make persist-settings' so incremental rebuilds remember the OpenSSL location. It alters no server, command, replication, persistence, or tool behaviour — given identical bu
- [valkey-io/valkey#4704](https://github.com/valkey-io/valkey/pull/4704) ([tests](tests/valkey/4704-7b87fee26/)): Preserve expired but unreaped fields across listpack-to-hashtable conversion (#4704): 6 of 6 test(s) reached the change, coverage 100%; crash, fixed by #4704 (recorded)
- [valkey-io/valkey#4712](https://github.com/valkey-io/valkey/pull/4712) ([tests](tests/valkey/4712-b4a976f68/)): Fix execGetKeys() dropping the first 256 keys past the static buffer (#4712): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4702](https://github.com/valkey-io/valkey/pull/4702) ([tests](tests/valkey/4702-00a8a1955/)): Attribute LZ4 codec context memory to client memory usage (#4702): 0 of 6 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4716](https://github.com/valkey-io/valkey/pull/4716) ([tests](tests/valkey/4716-e082696cf/)): Document VALKEYMODULE_OPTIONS_HANDLE_FORKLESS (#4716): declined: The diff touches only comments in src/module.c: it adds documentation for VALKEYMODULE_OPTIONS_HANDLE_FORKLESS above VM_SetModuleOptions and corrects a stale flag name (HANDLE_FORKLESS_SAVE -> HANDLE_FORKLESS) in another comment. No code, constant, or behaviour changes, so there is no path
- [valkey-io/valkey#4714](https://github.com/valkey-io/valkey/pull/4714) ([tests](tests/valkey/4714-f93169d8b/)): Deflake BgIterationTest.createAndCleanup (#4714): declined: The change modifies only src/unit/test_bgiteration.cpp, loosening a timing assertion (runtime_ms < 5 to < 1000) in the BgIterationTest.createAndCleanup unit test to deflake it in CI. It is a test-only change with no effect on Valkey server behavior, commands, replication, persistence, or t
- [valkey-io/valkey#4654](https://github.com/valkey-io/valkey/pull/4654) ([tests](tests/valkey/4654-5c24e5d20/)): [Bug Fix] Echo the rejected request epoch in FAILOVER_AUTH_NACK (#4654): 0 of 9 test(s) reached the change, coverage 64%, 1 skipped (required capability 'host' is unsupported)
- [valkey-io/valkey#4709](https://github.com/valkey-io/valkey/pull/4709) ([tests](tests/valkey/4709-bf8031274/)): Regenerate commands.def for the path hash command tips (#4709): 0 of 2 test(s) reached the change
- [valkey-io/valkey#4703](https://github.com/valkey-io/valkey/pull/4703) ([tests](tests/valkey/4703-72049a302/)): Report the real error when a reply schema fails validation (#4703): declined: The change modifies only utils/req-res-log-validator.py, a Python developer/test utility that validates reply-schema logs; it alters error reporting in that script, not any behaviour of valkey-server, replication, cluster, or the shipped tools. The fuzzer exercises real servers and Valkey 

### valkey-io/valkey-bloom

[2026-11-05T23:28:05+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42126539353): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-05T22:25:42+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42121304103): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-05T21:29:24+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42079311522): 20 merge(s) tested, 5 without a code change.

- [valkey-io/valkey-search#1364](https://github.com/valkey-io/valkey-search/pull/1364) ([tests](tests/valkey-search/1364-bd6d2f238/)): fixing redis search divergence #4: sort key prefix always use #; strings should use $ (#1364): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1084](https://github.com/valkey-io/valkey-search/pull/1084) ([tests](tests/valkey-search/1084-9e8fec7e0/)): Implement Renaming in LOAD clause of FT.AGGREGATE (COMPATIBILTY) PLUS FIX JSON on LOAD clause (#1084): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1196](https://github.com/valkey-io/valkey-search/pull/1196) ([tests](tests/valkey-search/1196-6e38de5d7/)): Allow indexing the same field as both TEXT and TAG (#1195) (#1196): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1331](https://github.com/valkey-io/valkey-search/pull/1331) ([tests](tests/valkey-search/1331-0190919f1/)): Fix ThreadPool suspend/resume race with concurrent Resize (#1331): 0 of 5 test(s) reached the change
- [valkey-io/valkey-search#1352](https://github.com/valkey-io/valkey-search/pull/1352) ([tests](tests/valkey-search/1352-bc55ab18c/)): fix breaking changes introduced in #1008 (#1352): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#949](https://github.com/valkey-io/valkey-search/pull/949) ([tests](tests/valkey-search/949-7296a5a1b/)): feat: add Random Sample reducer to FT.Aggregate groupby stage (#949): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search#1341](https://github.com/valkey-io/valkey-search/pull/1341) ([tests](tests/valkey-search/1341-7f04b7d6e/)): Implicitly load fields referenced by a pipeline stage (#919) (#1341): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1349](https://github.com/valkey-io/valkey-search/pull/1349) ([tests](tests/valkey-search/1349-e8f05bbd9/)): Fix HNSW load crash: remove incorrect max_level <= element_count check (#1349): 0 of 3 test(s) reached the change
- [valkey-io/valkey-search#759](https://github.com/valkey-io/valkey-search/pull/759) ([tests](tests/valkey-search/759-3fe91fb61/)): Handle ipv6 in coordinator (#759): declined: The change only affects gRPC target-address formatting in the valkey-search coordinator, and no fuzzer scenario can reach it. (1) All three changed call sites run only when the coordinator is enabled: ValkeySearch::Startup creates the MetadataManager, client pool and fanout paths only unde
- [valkey-io/valkey-search#1343](https://github.com/valkey-io/valkey-search/pull/1343) ([tests](tests/valkey-search/1343-b747be659/)): Fix aarch64 RDB compatibility integration tests (#1343): declined: The change only fixes the project's own integration-test harness and build scripts: it adds an aarch64 prebuilt 1.0.0 module fixture to a test zip and makes the Python RDB-compatibility test pick the fixture per architecture, moves AI-assistant skill docs, and corrects an inverted sanitize
- [valkey-io/valkey-search#1075](https://github.com/valkey-io/valkey-search/pull/1075) ([tests](tests/valkey-search/1075-293b0f27a/)): Fix TAG indexing of non UTF-8 data (#1075): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1287](https://github.com/valkey-io/valkey-search/pull/1287) ([tests](tests/valkey-search/1287-74037e9df/)): Scoring: BM25STD for text, numeric and tag (#1287): 4 of 4 test(s) reached the change; [regressed-fix, the fix of #1287 regressed](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/68)
- [valkey-io/valkey-search#1273](https://github.com/valkey-io/valkey-search/pull/1273) ([tests](tests/valkey-search/1273-6a5644a23/)): Fix FT.AGGREGATE alias re-use corrupting record state (#1251) (#1273): declined: no proposed test validated
- [valkey-io/valkey-search#1316](https://github.com/valkey-io/valkey-search/pull/1316) ([tests](tests/valkey-search/1316-7005c00c8/)): introduce VectorRegistry for memory sharing (#1316): 6 of 6 test(s) reached the change; crash, fixed by #1316 (recorded)
- [valkey-io/valkey-search#1232](https://github.com/valkey-io/valkey-search/pull/1232) ([tests](tests/valkey-search/1232-5662dc10c/)): Changing HNSWLib to store raw vectors (#1232): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1168](https://github.com/valkey-io/valkey-search/pull/1168) ([tests](tests/valkey-search/1168-0f3c267cd/)): Hnswlib templatized vectors (#1168): declined: no proposed test validated
- [valkey-io/valkey-search#971](https://github.com/valkey-io/valkey-search/pull/971) ([tests](tests/valkey-search/971-cbad9d689/)): change ft.aggregate to generate the Reducer name the same as Redisearch (#971): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1267](https://github.com/valkey-io/valkey-search/pull/1267) ([tests](tests/valkey-search/1267-66d1c822f/)): Allow SORTBY on vector score in ascending order (#1267): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search@cae4fd2dbb14](https://github.com/valkey-io/valkey-search/commit/cae4fd2dbb14189dcd7326366e9ff203d58d1ae7) ([tests](tests/valkey-search/cae4fd2db/)): Revert "Fix indexing for zero-length keys (#1171)": 4 of 4 test(s) reached the change
- [valkey-io/valkey-search#1264](https://github.com/valkey-io/valkey-search/pull/1264) ([tests](tests/valkey-search/1264-e415fe131/)): Fix numeric field precision loss in content materialization (#1264): 2 of 2 test(s) reached the change

## Totals

- Merges whose tests reached the change: 221 of 291 tested (76%)
- Changed-line coverage: median 100% of 120 merge(s) measured, 7 under 50%
- Recall: 66 parents flagged of 85 fix merges
- Claude: $1753.67 in the last 24 hours, $1765.75 in the last 7 days, $1765.75 in all
- Issues filed: 27
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

## Retries

None.
