# Fuzzer walk

Updated 2026-11-08T03:30+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 840 | 1667 | #2964 (2026-01-05) | 328 | 120.0 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 551 | 118 | #207 (2025-07-08) | 200 | 78.7 |

## Last runs

### valkey-io/valkey

[2026-11-08T01:57:46+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42423729771): 20 merge(s) tested, 8 without a code change.

- [valkey-io/valkey#3079](https://github.com/valkey-io/valkey/pull/3079) ([tests](tests/valkey/3079-8fdc9e657/)): Fix valkey-benchmark hanging in single-thread duration mode (#3079): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3075](https://github.com/valkey-io/valkey/pull/3075) ([tests](tests/valkey/3075-5e65bad9c/)): Cleanup and break the while loop in clusterPrimariesHaveReplicas (#3075): declined: The change is a pure refactor: clusterPrimariesHaveReplicas previously summed num_replicas over all primaries and returned (sum != 0); it now returns 1 at the first primary with num_replicas != 0. Because num_replicas is a non-negative count, the two forms compute the identical boolean for
- [valkey-io/valkey#3051](https://github.com/valkey-io/valkey/pull/3051) ([tests](tests/valkey/3051-9735bac65/)): Update dual channel replication conf to mention primary should also enable repl-diskless-sync (#3051): 0 of 8 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2931](https://github.com/valkey-io/valkey/pull/2931) ([tests](tests/valkey/2931-4524c2352/)): Cumulative metric for active main thread usage (#2931): 4 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2984](https://github.com/valkey-io/valkey/pull/2984) ([tests](tests/valkey/2984-bdc62355c/)): Add ValkeyModule_ClusterKeySlotC  (#2984): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3005](https://github.com/valkey-io/valkey/pull/3005) ([tests](tests/valkey/3005-2635377f4/)): Fix used_memory_dataset underflow due to miscalculated used_memory_overhead (#3005): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3048](https://github.com/valkey-io/valkey/pull/3048) ([tests](tests/valkey/3048-fa5964312/)): Fix HEXPIRE should not delete items when validation rules fail and expiration is in the past (#3048): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3046](https://github.com/valkey-io/valkey/pull/3046) ([tests](tests/valkey/3046-b7b11d77d/)): Avoid duplicate calculations of network-bytes-out in slot stats with copy-avoidance (#3046): 3 of 3 test(s) reached the change
- [valkey-io/valkey#3055](https://github.com/valkey-io/valkey/pull/3055) ([tests](tests/valkey/3055-b7f1a6c7d/)): Delete large binary junk file valkey-microbench (#3055): declined: The change only deletes src/valkey-microbench, an accidentally committed binary artifact. No source code, build rules, tools, or runtime behaviour change, so there is no path for a scenario to exercise.
- [valkey-io/valkey#3030](https://github.com/valkey-io/valkey/pull/3030) ([tests](tests/valkey/3030-f0d981014/)): Don't stop the dirty scripts when an OOM occurs midway through execution (#3030): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3040](https://github.com/valkey-io/valkey/pull/3040) ([tests](tests/valkey/3040-3c546e2c3/)): Makes `CLUSTER KEYSLOT` available in standalone mode (#3040): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3023](https://github.com/valkey-io/valkey/pull/3023) ([tests](tests/valkey/3023-6865f0a6e/)): Fix HEXPIRE should not delete items when GT rule is used and expiration is in the past (#3023): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3004](https://github.com/valkey-io/valkey/pull/3004) ([tests](tests/valkey/3004-de0005432/)): Skip slot cache optimization for AOF client to prevent key duplication and data corruption (#3004): 1 of 1 test(s) reached the change, coverage 100%; crash, predates #3004, since fixed (recorded); crash, fixed by #3004 (recorded)
- [valkey-io/valkey#2777](https://github.com/valkey-io/valkey/pull/2777) ([tests](tests/valkey/2777-f85ec7b00/)): Add logging helper function to print node's ip:port when nodename not explicitly set (#2777): 0 of 5 test(s) reached the change, coverage 81%, 2 skipped (required capability 'save-behavior-support' is unsupported: default: build rejects startup configuration: *** FATAL CONFIG FILE ERROR (Version 255.255.255) *** Reading the configuration file, at line )
- [valkey-io/valkey#2945](https://github.com/valkey-io/valkey/pull/2945) ([tests](tests/valkey/2945-1d23866b7/)): Trigger prepareCommand on argc change in module command filters (#2945): declined: The change only executes inside moduleCallCommandFilters (src/module.c:11242), which does nothing unless a module has registered a command filter; it re-runs prepareCommand (recomputing arity and the cluster slot) when a filter changes argc, not just when it changes the command name. The o
- [valkey-io/valkey#1791](https://github.com/valkey-io/valkey/pull/1791) ([tests](tests/valkey/1791-269f6e708/)): Adding json support for log-format config (#1791): 3 of 6 test(s) reached the change, coverage 95%
- [valkey-io/valkey#3006](https://github.com/valkey-io/valkey/pull/3006) ([tests](tests/valkey/3006-5a3fb0454/)): HFE make zero a valid ttl during import mode and data loading  (#3006): 4 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2516](https://github.com/valkey-io/valkey/pull/2516) ([tests](tests/valkey/2516-0ee423450/)): Remove internal server object pointer overhead in small strings (#2516): 2 of 26 test(s) reached the change, coverage 34%, 1 skipped (required capability 'build' is unsupported: default: build rejects startup configuration: *** FATAL CONFIG FILE ERROR (Version 255.255.255) *** Reading the configuration file, at line 11 >>> 'forkless); check, predates #2516, since fixed (recorded)
- [valkey-io/valkey#2986](https://github.com/valkey-io/valkey/pull/2986) ([tests](tests/valkey/2986-9f7cfc771/)): Refactor clusterMsg type casting for improve type safety (#2986): 0 of 6 test(s) reached the change, coverage 71%, 1 skipped (required capability 'save-behavior-support' is unsupported: default: build rejects startup configuration: *** FATAL CONFIG FILE ERROR (Version 255.255.255) *** Reading the configuration file, at line )
- [valkey-io/valkey#2987](https://github.com/valkey-io/valkey/pull/2987) ([tests](tests/valkey/2987-96c6cb841/)): fix asprintf unused result warn (#2987): declined: The change is a mechanical refactor to silence a -Wunused-result build warning: six call sites in the Lua debugger (src/modules/lua/debug_lua.c) swap asprintf+free for lm_asprintf+ValkeyModule_Free. lm_asprintf (script_lua.c:235) produces byte-identical formatted strings via vsnprintf into

### valkey-io/valkey-bloom

[2026-11-08T03:30:41+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42442971551): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-08T03:22:11+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42442381193): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-08T03:09:30+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42430594824): 20 merge(s) tested, 1 without a code change.

- [valkey-io/valkey-search#218](https://github.com/valkey-io/valkey-search/pull/218) ([tests](tests/valkey-search/218-64dfbc989/)): Adding support for non vector queries (#218): 4 of 4 test(s) reached the change; crash, introduced and since fixed (recorded)
- [valkey-io/valkey-search#243](https://github.com/valkey-io/valkey-search/pull/243) ([tests](tests/valkey-search/243-91035f6f8/)): Fix attribute alias handling in filters when running in CME mode (#243): 1 of 1 test(s) reached the change
- [valkey-io/valkey-search#244](https://github.com/valkey-io/valkey-search/pull/244) ([tests](tests/valkey-search/244-61a310b1c/)): Refactor configs (#244): 2 of 2 test(s) reached the change; crash, predates #244, since fixed (recorded)
- [valkey-io/valkey-search#240](https://github.com/valkey-io/valkey-search/pull/240) ([tests](tests/valkey-search/240-1c220e758/)): Add metric for reclaimable memory (#240): 3 of 3 test(s) reached the change; crash, predates #240, since fixed (recorded); crash, introduced and since fixed (recorded); crash, fixed by #240 (recorded)
- [valkey-io/valkey-search#236](https://github.com/valkey-io/valkey-search/pull/236) ([tests](tests/valkey-search/236-5ff98b6dd/)): Supporting tsan build and addressing the time slice mutex test issues (#236): 0 of 4 test(s) reached the change; crash, predates #236, since fixed (recorded)
- [valkey-io/valkey-search#238](https://github.com/valkey-io/valkey-search/pull/238) ([tests](tests/valkey-search/238-54f428083/)): Improve unit testing cmake infrastructure (#238): declined: The change only restructures the unit-test CMake build (consolidating per-file test executables into six suite executables, splitting testing_common into testing_common_base and testing_common_coordinator, and deleting the absorbed testing/coordinator, testing/query and testing/utils CMake
- [valkey-io/valkey-search#239](https://github.com/valkey-io/valkey-search/pull/239) ([tests](tests/valkey-search/239-cdd08c287/)): Limit the result record in ft.search cmd (#239): 3 of 3 test(s) reached the change; crash, predates #239, since fixed (recorded)
- [valkey-io/valkey-search#231](https://github.com/valkey-io/valkey-search/pull/231) ([tests](tests/valkey-search/231-42970c7c3/)): Add limit to the K value in FT.SEARCH cmd (#231): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search#229](https://github.com/valkey-io/valkey-search/pull/229) ([tests](tests/valkey-search/229-35dffab23/)): Add a limit on the number of terms in a search query (#229): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search#230](https://github.com/valkey-io/valkey-search/pull/230) ([tests](tests/valkey-search/230-b7cdc9fc2/)): Enhanced Metrics for Valkey Search Ingestion (#230): 5 of 5 test(s) reached the change; crash, predates #230, since fixed (recorded)
- [valkey-io/valkey-search#228](https://github.com/valkey-io/valkey-search/pull/228) ([tests](tests/valkey-search/228-5c4b578a1/)): Added missing "String" configuration entry (#228): declined: The change adds a new String configuration entry class to the vmsdk module-config framework, but at this commit no valkey-search code registers a String config: the only instantiation is the new unit test (vmsdk/testing/configuration_test.cc), and all runtime configs remain Number/Boolean/
- [valkey-io/valkey-search#222](https://github.com/valkey-io/valkey-search/pull/222) ([tests](tests/valkey-search/222-553db36de/)): Add query string bytes limit using a module config (#222): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search@4051589915be](https://github.com/valkey-io/valkey-search/commit/4051589915be79b07e6630aae4662e352200453a) ([tests](tests/valkey-search/405158991/)): Merge branch 'fulltext' into main: declined: The merge adds only non-functional content: a cspell word-list update, Markdown design docs under docs/full-text/ with a doc-scraping script, and skeleton C++ headers (src/indexes/text.h, src/indexes/text/*.h, src/indexes/text_index.h) sketching a future full-text index. No CMake/Bazel fil
- [valkey-io/valkey-search#221](https://github.com/valkey-io/valkey-search/pull/221) ([tests](tests/valkey-search/221-ecc807434/)): Cripple broken unit test (#221): declined: The change modifies only unit test files (testing/*.cc and vmsdk/testing/*.cc): it adds EXPECT_CALL mock expectations for GetIdentifier in several gmock-based tests and disables two flaky tests (LogTest.SinkOptions, ThreadPoolTest.priority) behind #ifdef BROKEN_UNIT_TEST. No code that is c
- [valkey-io/valkey-search#220](https://github.com/valkey-io/valkey-search/pull/220) ([tests](tests/valkey-search/220-436e5b04c/)): Fix duplicated code (#220): declined: The change deletes dead code only: in SchemaManager::CreateIndexSchema, the removed `if (GetNumberOfIndexSchemas() >= max_indexes)` check was unreachable, because the preceding VMSDK_RETURN_IF_ERROR(vmsdk::VerifyRange(count + 1, nullopt, max_indexes)) fires on exactly the same condition (c
- [valkey-io/valkey-search#217](https://github.com/valkey-io/valkey-search/pull/217) ([tests](tests/valkey-search/217-78e4f65bd/)): Add CPU metrics for reader and writer threads (#217): 0 of 3 test(s) reached the change; crash, introduced and since fixed (recorded)
- [valkey-io/valkey-search#214](https://github.com/valkey-io/valkey-search/pull/214) ([tests](tests/valkey-search/214-a932c9bbf/)): Add operational limits to create command (#214): 4 of 4 test(s) reached the change
- [valkey-io/valkey-search#212](https://github.com/valkey-io/valkey-search/pull/212) ([tests](tests/valkey-search/212-a69edd54d/)): Add network byte counting to coordinator client (#212): declined: The change only increments two new atomic byte counters inside the coordinator gRPC client's SearchIndexPartition and prints them as INFO fields gated by UsingCoordinator(). That code runs only when the module's use-coordinator option is enabled, and that option is a hidden config (VALKEYM
- [valkey-io/valkey-search#216](https://github.com/valkey-io/valkey-search/pull/216) ([tests](tests/valkey-search/216-a0e527508/)): Fix DisplayAsSIBytes test (#216): declined: The change modifies a single assertion in vmsdk/testing/utils_test.cc, changing EXPECT_EQ(buffer[1], -1) to EXPECT_EQ(buffer[1], '\xff') to fix the test under platforms where char is unsigned. It touches only test code: no module source, command handling, indexing, replication or persisten
- [valkey-io/valkey-search#182](https://github.com/valkey-io/valkey-search/pull/182) ([tests](tests/valkey-search/182-28462b09b/)): Limit FT.SEARCH query string parsing recursive depth by a module config (#182): 2 of 2 test(s) reached the change

## Totals

- Merges whose tests reached the change: 544 of 785 tested (69%)
- Changed-line coverage: median 100% of 378 merge(s) measured, 24 under 50%
- Recall: 153 parents flagged of 225 fix merges
- Claude: $1900.39 in the last 24 hours, $5608.54 in the last 7 days, $5608.54 in all
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
