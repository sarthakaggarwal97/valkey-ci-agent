# Fuzzer walk

Updated 2026-11-06T13:20+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 338 | 2160 | #4401 (2026-08-13) | 225 | 48.3 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 32 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 255 | 413 | #903 (2026-03-13) | 203 | 36.4 |

## Last runs

### valkey-io/valkey

[2026-11-06T12:45:51+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42193821279): 20 merge(s) tested, 6 without a code change.

- [valkey-io/valkey#5251](https://github.com/valkey-io/valkey/pull/5251) ([tests](tests/valkey/5251-eda518abf/)): Fix CLUSTER GETSLOTMIGRATIONS reply schema (#5251): declined: The change edits only src/commands/cluster-getslotmigrations.json, the reply-schema metadata: it renames the documented field 'link_name' to 'name' and documents the existing 'remaining_repl_size' field. The server's actual reply in clusterCommandGetSlotMigrations (cluster_migrateslots.c) 
- [valkey-io/valkey#5289](https://github.com/valkey-io/valkey/pull/5289) ([tests](tests/valkey/5289-c77a4e8da/)): Document missing key types and notification flags in module API (#5289): declined: The change is documentation-only: it edits two doc comments in src/module.c (listing the key type constants returned by VM_KeyType and the notification flags accepted by SubscribeToKeyspaceEvents). No code, constant values, replies, or behavior change, so no scenario can distinguish the ch
- [valkey-io/valkey#4522](https://github.com/valkey-io/valkey/pull/4522) ([tests](tests/valkey/4522-4ac65b3d8/)): Refactor: Extract evictSingleKey() from performEvictions() (#4522): declined: This is a pure behavior-preserving refactor: it moves the per-key eviction body of performEvictions() verbatim into a new static helper evictSingleKey() in src/evict.c. Every operation (execution-unit bracketing, memory-delta measurement around dbGenericDelete, latency sampling, stat_evict
- [valkey-io/valkey#4492](https://github.com/valkey-io/valkey/pull/4492) ([tests](tests/valkey/4492-09bd34780/)): Refactor internal AE polling API to not pass aeEventLoop to backends (#4492): 0 of 11 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4472](https://github.com/valkey-io/valkey/pull/4472) ([tests](tests/valkey/4472-382a13495/)): Rewrite ready-key UAF fix with rotation-based iteration (#4472): 0 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4459](https://github.com/valkey-io/valkey/pull/4459) ([tests](tests/valkey/4459-52a98359d/)): Fix ORDERBY truncation in CLUSTER SLOT-STATS comparators (#4459): declined: The fix only changes CLUSTER SLOT-STATS ORDERBY output when two slots' ordering statistic (key-count, cpu-usec, network-bytes-in/out) differ by roughly 2^31 or more, since the bug was a uint64_t subtraction truncated to int in the qsort comparators; below that threshold both the parent and
- [valkey-io/valkey#4488](https://github.com/valkey-io/valkey/pull/4488) ([tests](tests/valkey/4488-3f0d910c8/)): Minor cleanup to update cluster-node-timeout config name references (#4488): declined: The change only edits human-readable text: a log message string in valkey-cli's clusterManagerCommandSetTimeout error path (renaming 'node-timeout' to 'cluster-node-timeout' in the message) and comments/example numbers in valkey.conf. No executable behaviour, command reply, config parsing,
- [valkey-io/valkey#4318](https://github.com/valkey-io/valkey/pull/4318) ([tests](tests/valkey/4318-13e6aa835/)): build: pass CPPFLAGS to bundled deps and src (#4318): declined: The change is purely to the build system: it appends $(CPPFLAGS) to the compiler invocation lines in the Makefiles of bundled deps (fpconv, hdr_histogram, linenoise), src/, and the Lua module, and persists CPPFLAGS in .make-settings. It adds no code, changes no command, reply, replication,
- [valkey-io/valkey#4454](https://github.com/valkey-io/valkey/pull/4454) ([tests](tests/valkey/4454-11188c239/)): Persist non-zero cluster-replica-priority to nodes.conf as an aux field (#4454): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4469](https://github.com/valkey-io/valkey/pull/4469) ([tests](tests/valkey/4469-0fb02b7eb/)): Fix include order in custom_matchers.hpp to prevent min/max macro pollution (#4469): declined: The change only reorders gmock/gtest #include directives in unit-test headers (src/unit/custom_matchers.hpp and src/unit/wrappers.h) to avoid min/max macro pollution when compiling the C++ unit tests. It is a build/test-infrastructure fix with no change to server, replication, persistence,
- [valkey-io/valkey#4395](https://github.com/valkey-io/valkey/pull/4395) ([tests](tests/valkey/4395-12de25e8d/)): Fix mem_clients_type_memory leak for cached primary client (#4395): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4440](https://github.com/valkey-io/valkey/pull/4440) ([tests](tests/valkey/4440-36aca4bfb/)): Skip IO-thread write-done client re-lookup unless update_state sync-invokes handlers (#4440): 0 of 6 test(s) reached the change, coverage 50%
- [valkey-io/valkey#3118](https://github.com/valkey-io/valkey/pull/3118) ([tests](tests/valkey/3118-5d4f9d670/)): Incr the dirty counter when deleting expired keys/fields from active expire (#3118): 2 of 9 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3984](https://github.com/valkey-io/valkey/pull/3984) ([tests](tests/valkey/3984-413f6c32a/)): AOF loading must not check ACL permissions (#3984): 2 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3531](https://github.com/valkey-io/valkey/pull/3531) ([tests](tests/valkey/3531-5d3fd6858/)): Add streaming compression support for RDB (#3531): 5 of 18 test(s) reached the change, coverage 77%, 2 skipped (required capability 'build' is unsupported)
- [valkey-io/valkey#3531](https://github.com/valkey-io/valkey/pull/3531) ([tests](tests/valkey/3531-2d7f3d9fd/)): Vendor LZ4 1.10.0 (#3531): declined: The change only vendors the LZ4 1.10.0 sources into deps/lz4/ and adds them to the deps build (deps/Makefile, deps/CMakeLists.txt), plus README, LICENSE, REUSE.toml and typos.toml updates. No file under src/ references LZ4: the server, tools and tests neither compile nor link the new libra
- [valkey-io/valkey#4415](https://github.com/valkey-io/valkey/pull/4415) ([tests](tests/valkey/4415-cd6290354/)): Fix signal mask parsing on 32-bit systems (#4415): declined: This is a 32-bit-only correctness fix with no observable effect on the 64-bit servers the fuzzer runs. The widened type (unsigned long -> unsigned long long) and 1L -> 1ULL shift only matter where `unsigned long` is 32 bits; on a 64-bit (x86_64) build `unsigned long` is already 64 bits, so
- [valkey-io/valkey#4197](https://github.com/valkey-io/valkey/pull/4197) ([tests](tests/valkey/4197-a3c5f440e/)): Track full sync completion time (#4197): 2 of 5 test(s) reached the change, coverage 92%
- [valkey-io/valkey#4410](https://github.com/valkey-io/valkey/pull/4410) ([tests](tests/valkey/4410-4e98093b2/)): Fix out-of-bounds access in cluster message receivers (#4410): 0 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4267](https://github.com/valkey-io/valkey/pull/4267) ([tests](tests/valkey/4267-917de6ac1/)): Log EXEC in commandlog (#4267): 2 of 2 test(s) reached the change

### valkey-io/valkey-bloom

[2026-11-06T13:20:46+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42224657130): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-06T13:20:19+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42224609001): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-06T12:55:05+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42201766408): 20 merge(s) tested, 7 without a code change.

- [valkey-io/valkey-search#939](https://github.com/valkey-io/valkey-search/pull/939) ([tests](tests/valkey-search/939-dcc1214f1/)): Shorten grpc timeout (#939): 1 of 1 test(s) reached the change
- [valkey-io/valkey-search#942](https://github.com/valkey-io/valkey-search/pull/942) ([tests](tests/valkey-search/942-1e5c7459a/)): Skip building unit test files for integration test workflows (#942): declined: The change only touches build tooling: CMakeLists.txt renames the BUILD_TESTS option to BUILD_UNIT_TESTS, build.sh passes the renamed flag and allows CMAKE_EXTRA_ARGS from the environment, and ci/build_ubuntu.sh disables unit-test compilation when a CI run executes integration tests only. 
- [valkey-io/valkey-search#941](https://github.com/valkey-io/valkey-search/pull/941) ([tests](tests/valkey-search/941-6362bc1b0/)): Fix RDB load memory leak (#941): declined: no proposed test validated
- [valkey-io/valkey-search#934](https://github.com/valkey-io/valkey-search/pull/934) ([tests](tests/valkey-search/934-0b51c5595/)): Add memory reservation based on mutation queue (#934): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#926](https://github.com/valkey-io/valkey-search/pull/926) ([tests](tests/valkey-search/926-ef1f6193a/)): Allow replacement of deleted hnsw elements (#926): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search#931](https://github.com/valkey-io/valkey-search/pull/931) ([tests](tests/valkey-search/931-03f66fa81/)): Improve test output (#931): declined: The change modifies only build.sh (build/test-runner script: ICU build output redirected to a log file with failure tails, and per-test output files so only the failed test's output is dumped) and removes std::cout debug prints from unit tests in testing/coordinator/metadata_manager_test.c
- [valkey-io/valkey-search#930](https://github.com/valkey-io/valkey-search/pull/930) ([tests](tests/valkey-search/930-d1bf81158/)): Adding yield during mutation queue drainage for save and restore (#930): 0 of 3 test(s) reached the change
- [valkey-io/valkey-search#921](https://github.com/valkey-io/valkey-search/pull/921) ([tests](tests/valkey-search/921-de0bb244d/)): Slot Ownership check (#921): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#860](https://github.com/valkey-io/valkey-search/pull/860) ([tests](tests/valkey-search/860-e972c8f71/)): Some tsan fixes (#860): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#827](https://github.com/valkey-io/valkey-search/pull/827) ([tests](tests/valkey-search/827-eff3d337c/)): adding the commented out tests (#827): declined: The change only edits valkey-search's own Python integration test suite (testing/integration/stability_test.py): it re-enables three previously commented-out stability test cases (text/tag/numeric with backfill, coordinator and replica) and turns on background ftcreate/ftdropindex/flushdb 
- [valkey-io/valkey-search#871](https://github.com/valkey-io/valkey-search/pull/871) ([tests](tests/valkey-search/871-4fb4b5fee/)): Reducing mutation queue growth during restore (#871): 3 of 3 test(s) reached the change; crash, predates #871, since fixed (recorded)
- [valkey-io/valkey-search#913](https://github.com/valkey-io/valkey-search/pull/913) ([tests](tests/valkey-search/913-4293fddfd/)): Mark ft info metrics as Dev configs (#913): declined: no proposed test validated
- [valkey-io/valkey-search#912](https://github.com/valkey-io/valkey-search/pull/912) ([tests](tests/valkey-search/912-a2d6da892/)): Fix RDB Load String Memory Leak (#912): 1 of 1 test(s) reached the change; crash, introduced and since fixed (recorded); crash, fixed by #912 (recorded)
- [valkey-io/valkey-search#911](https://github.com/valkey-io/valkey-search/pull/911) ([tests](tests/valkey-search/911-e7b9d6359/)): Change search.hide-user-data-from-log config to Dev and remove total_indexing_time (unimplemented metric) (#911): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#883](https://github.com/valkey-io/valkey-search/pull/883) ([tests](tests/valkey-search/883-fe5945889/)): Implement clean up index schemas on SHUTDOWN event (#883): 0 of 3 test(s) reached the change; crash, predates #883, since fixed (recorded)
- [valkey-io/valkey-search#905](https://github.com/valkey-io/valkey-search/pull/905) ([tests](tests/valkey-search/905-c4fb75f92/)): [DOCUMENTATION] Application Metrics Cleanup (#905): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#909](https://github.com/valkey-io/valkey-search/pull/909) ([tests](tests/valkey-search/909-b9325e833/)): Add history entries for text indexing and search support in v1.2.0 (#909): declined: The change only adds "history" entries ("1.2.0", "Added Text Indexing and Search support") to four command-documentation JSON files (src/commands/ft.aggregate.json, ft.create.json, ft.info.json, ft.search.json). These files are not consumed by the build: src/commands/CMakeLists.txt compile
- [valkey-io/valkey-search#873](https://github.com/valkey-io/valkey-search/pull/873) ([tests](tests/valkey-search/873-7e9d0d6ea/)): Search module - Audit log messages (#873): declined: The change only edits log messages in the valkey-search module: log levels are moved between NOTICE and DEBUG, several logs are rate-limited with VMSDK_LOG_EVERY_N_SEC, error-message details and DEBUG/FANOUT_DEBUG prefixes are removed from log lines for audit purposes, and two test files d
- [valkey-io/valkey-search#906](https://github.com/valkey-io/valkey-search/pull/906) ([tests](tests/valkey-search/906-ea3bfdcbb/)): Add validation to reject attribute aliases containing {}[]:;$ (#906): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#896](https://github.com/valkey-io/valkey-search/pull/896) ([tests](tests/valkey-search/896-1045bb0e0/)): Implement configuration for TAG wildcard minimum prefix length (#896): 2 of 2 test(s) reached the change

## Totals

- Merges whose tests reached the change: 301 of 412 tested (73%)
- Changed-line coverage: median 100% of 178 merge(s) measured, 8 under 50%
- Recall: 89 parents flagged of 124 fix merges
- Claude: $1840.56 in the last 24 hours, $2778.47 in the last 7 days, $2778.47 in all
- Issues filed: 32
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

## Retries

None.
