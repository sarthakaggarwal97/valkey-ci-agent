# Fuzzer walk

Updated 2026-11-06T00:21+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 233 | 2259 | #4506 (2026-09-16) | 231 | 33.3 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 107 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 56 | 8.9 |
| valkey-io/valkey-search | 147 | 521 | #1207 (2026-07-01) | 147 | 21.0 |

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

[2026-11-05T23:58:44+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42133340873): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-05T23:55:46+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42132975406): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-06T00:21:55+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42113110732): 20 merge(s) tested, 2 without a code change.

- [valkey-io/valkey-search#946](https://github.com/valkey-io/valkey-search/pull/946) ([tests](tests/valkey-search/946-a5233dfd9/)): feat: add First Value reducer to FT.Aggregate groupby stage (#946): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search#1171](https://github.com/valkey-io/valkey-search/pull/1171) ([tests](tests/valkey-search/1171-1a2306b10/)): Fix indexing for zero-length keys (#1171): 3 of 3 test(s) reached the change; crash, introduced and since fixed (recorded)
- [valkey-io/valkey-search#1281](https://github.com/valkey-io/valkey-search/pull/1281) ([tests](tests/valkey-search/1281-8fc3bbdc0/)): Lower per-field info definition log from WARNING to DEBUG (#1281): declined: The change only lowers the log level of a per-field 'Defined Info Field' diagnostic message from WARNING to DEBUG in vmsdk/src/info.cc. It alters no command behavior, replies, data, replication, or configuration — just server log verbosity, which the fuzzer's oracles do not observe. There 
- [valkey-io/valkey-search#1265](https://github.com/valkey-io/valkey-search/pull/1265) ([tests](tests/valkey-search/1265-bb1f1c723/)): Fix replica reporting incorrect search_number_of_indexes after full sync (#1265): declined: no proposed test validated
- [valkey-io/valkey-search#1230](https://github.com/valkey-io/valkey-search/pull/1230) ([tests](tests/valkey-search/1230-92efaa600/)): Fix SIGSEGV in LatencySampler when hdr_init fails (#1230): declined: The fix only changes behaviour when hdr_init fails inside LatencySampler::SubmitSample. In the shipped module every LatencySampler is constructed with hard-coded valid parameters (min=1ns, max=1s, precision=2 in src/metrics.h), so hdr_init can only fail on calloc returning NULL inside the 
- [valkey-io/valkey-search#1283](https://github.com/valkey-io/valkey-search/pull/1283) ([tests](tests/valkey-search/1283-459878064/)): Fix duplicate label handling (#1283): declined: The change fixes HNSW duplicate-label handling in two paths, both unreachable by valkey-fuzzer. (1) The modify-path fix (vector_hnsw.cc ModifyRecordImpl dropping markDelete, and hnswalg.h addPoint reusing an existing label's slot) only changes behavior when allow_replace_deleted_ is true. 
- [valkey-io/valkey-search#1172](https://github.com/valkey-io/valkey-search/pull/1172) ([tests](tests/valkey-search/1172-7a7b94538/)): Fix up test_skip_index_load.py (#1172): declined: The change modifies only a Python integration test (test_skip_index_load.py, refactored to use the test framework's create_server instead of raw subprocess) and a CI build script (scripts/common.rc, adding an ELF linker flag so ASAN sanitizer builds load libstdc++ early). No module source,
- [valkey-io/valkey-search#1276](https://github.com/valkey-io/valkey-search/pull/1276) ([tests](tests/valkey-search/1276-53ae957fd/)): feat: overload protection - 2 mechanisms to prevent an ever growing query queue  (#1276): 4 of 4 test(s) reached the change
- [valkey-io/valkey-search#932](https://github.com/valkey-io/valkey-search/pull/932) ([tests](tests/valkey-search/932-a1f2159c6/)): feat: add TOLIST reducer for FT.AGGREGATE GROUPBY stage (#932): 4 of 4 test(s) reached the change
- [valkey-io/valkey-search#1259](https://github.com/valkey-io/valkey-search/pull/1259) ([tests](tests/valkey-search/1259-578a75a37/)): fix hardcoded db_num in metadata_manager.cc (#1259): declined: The fix is inside coordinator::MetadataManager::OnLoadingEnded, which only exists when valkey-search is started in cluster mode with the --use-coordinator module argument (valkey_search.cc:1185 creates the MetadataManager only under GetUseCoordinator() && IsCluster()). That option can only
- [valkey-io/valkey-search#832](https://github.com/valkey-io/valkey-search/pull/832) ([tests](tests/valkey-search/832-320aec93e/)): Replace blocks with token field in json files (#832): declined: The change only rewrites the command-spec JSON metadata files (ft.aggregate.json, ft.create.json, ft.search.json) from nested pure-token blocks to the compact token-field notation. These files are documentation/metadata artifacts: they are not listed in any CMakeLists, not embedded in the 
- [valkey-io/valkey-search#1030](https://github.com/valkey-io/valkey-search/pull/1030) ([tests](tests/valkey-search/1030-e028db6c0/)): Reimplement Numeric field for 3x space reduction (#1030): 0 of 4 test(s) reached the change
- [valkey-io/valkey-search#1155](https://github.com/valkey-io/valkey-search/pull/1155) ([tests](tests/valkey-search/1155-3e3825c01/)): Revise handling of keys with fields with invalid values to match Redisearch (#1155): 1 of 5 test(s) reached the change
- [valkey-io/valkey-search#1086](https://github.com/valkey-io/valkey-search/pull/1086) ([tests](tests/valkey-search/1086-6f297274c/)): Fix crashes in expression evaluation. Add compatibility fixes. (#1086): 6 of 6 test(s) reached the change; crash, fixed by #1086 (recorded)
- [valkey-io/valkey-search#893](https://github.com/valkey-io/valkey-search/pull/893) ([tests](tests/valkey-search/893-79e1b3e7d/)): feat: Add array value type support to expression system (#893): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1231](https://github.com/valkey-io/valkey-search/pull/1231) ([tests](tests/valkey-search/1231-af1ca3ee8/)): Revise outdated text index comments (#1231): declined: The change only revises comments in rax_wrapper.h and text_index.h and deletes an unused private member (track_subtree_item_counts_) and its unused accessor (TrackSubtreeItemsCountEnabled) — a grep over the repository at the change shows no remaining references, so it was dead code. No run
- [valkey-io/valkey-search#1226](https://github.com/valkey-io/valkey-search/pull/1226) ([tests](tests/valkey-search/1226-a9a8b9ad2/)): Handle kOneReplicaPerShard in GetTargetsForSlot for single-slot indexes (#1226): declined: no proposed test validated
- [valkey-io/valkey-search#1222](https://github.com/valkey-io/valkey-search/pull/1222) ([tests](tests/valkey-search/1222-87978b530/)): The query vector is not normalized on the prefiltering query execution path (#1222): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1223](https://github.com/valkey-io/valkey-search/pull/1223) ([tests](tests/valkey-search/1223-9e51a16ec/)): Fixing debug mode compilation failure related to simsimd (#1223): declined: The change only adds `static` to three `inline` SIMD helper functions in vendored simsimd headers to fix a debug-mode (-O0) compilation/linkage failure. It changes symbol linkage, not any function body: distance and log2 computations, and thus all module commands and replies, are identical
- [valkey-io/valkey-search#1187](https://github.com/valkey-io/valkey-search/pull/1187) ([tests](tests/valkey-search/1187-e8e94d700/)): RemoveSubscription improper destruction handling fix (#1187): declined: This is a replica-only crash fix on the diskless-RDB-load error path, and the valkey-fuzzer has no way to reach or demonstrate it. (1) The bug requires a diskless replica full sync (src/index_schema.cc:1614 LoadFromRDB -> LoadIndexExtension), which only happens over replication with at lea

## Totals

- Merges whose tests reached the change: 229 of 300 tested (76%)
- Changed-line coverage: median 100% of 120 merge(s) measured, 7 under 50%
- Recall: 68 parents flagged of 87 fix merges
- Claude: $1823.65 in the last 24 hours, $1854.22 in the last 7 days, $1854.22 in all
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
