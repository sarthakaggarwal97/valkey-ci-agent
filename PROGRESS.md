# Fuzzer walk

Updated 2026-11-06T12:49+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 338 | 2160 | #4401 (2026-08-13) | 225 | 48.3 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 32 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 228 | 440 | #944 (2026-03-31) | 176 | 32.6 |

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

[2026-11-06T12:19:17+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42216683471): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-06T12:49:40+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42220563511): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-06T10:19:16+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42175828478): 20 merge(s) tested, 12 without a code change.

- [valkey-io/valkey-search#1037](https://github.com/valkey-io/valkey-search/pull/1037) ([tests](tests/valkey-search/1037-da3872fc8/)): Fix Reader Thread Scaling Bottlenecks due to PausePoint Mutex (#1037): 0 of 2 test(s) reached the change
- [valkey-io/valkey-search#1040](https://github.com/valkey-io/valkey-search/pull/1040) ([tests](tests/valkey-search/1040-7cb0d02c4/)): Use protobuf Arena for fanout response deserialization (#1040): declined: no proposed test validated
- [valkey-io/valkey-search#1029](https://github.com/valkey-io/valkey-search/pull/1029) ([tests](tests/valkey-search/1029-1b85d1acc/)): Fix RunByMain shutdown leak by draining pending one-shots (#1029): declined: no proposed test validated
- [valkey-io/valkey-search#945](https://github.com/valkey-io/valkey-search/pull/945) ([tests](tests/valkey-search/945-0fafddb0d/)): Use ccache to cache build object files to improve build time (#945): declined: The change only adds ccache to the build and CI pipeline: a Dockerfile package install, GitHub Actions cache-restore steps and CCACHE_DIR/CCACHE_MAXSIZE environment variables, and CMAKE_C/CXX_COMPILER_LAUNCHER settings in CMakeLists.txt. No module source code changes, so the built module's
- [valkey-io/valkey-search#1027](https://github.com/valkey-io/valkey-search/pull/1027) ([tests](tests/valkey-search/1027-1c30dd5b3/)): Fix crash in unittests introduced in previous PR (#1027): declined: The change modifies only unit-test files (testing/expr/expr_test.cc and testing/expr/value_test.cc), switching their gtest fixtures to inherit from vmsdk::ValkeyTest to fix a crash in the project's own test harness. No runtime code of the valkey-search module changes, so the module's behav
- [valkey-io/valkey-search#963](https://github.com/valkey-io/valkey-search/pull/963) ([tests](tests/valkey-search/963-70b8c2025/)): Fix timefmt causes OOM on empty input, Issue: #962 (#963): 2 of 2 test(s) reached the change; crash, fixed by #963 (recorded)
- [valkey-io/valkey-search#1003](https://github.com/valkey-io/valkey-search/pull/1003) ([tests](tests/valkey-search/1003-dcef76238/)): Ensure fixed ordering in the serialization of the fields. This matters for Text (#1003): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1007](https://github.com/valkey-io/valkey-search/pull/1007) ([tests](tests/valkey-search/1007-cabb67073/)): docker update test (#1007): declined: The change modifies only .devcontainer/Dockerfile, dropping the software-properties-common package and the ubuntu-toolchain-r/test PPA from the development container image used for local development/CI tooling. No module source, command behavior, replication, persistence, or runtime code p
- [valkey-io/valkey-search#994](https://github.com/valkey-io/valkey-search/pull/994) ([tests](tests/valkey-search/994-0f6d44d87/)): Fix Tag Search's EntriesFetcherIterator logic to not rely on RootIterator (full tree walk) on standard cases (#994): 0 of 2 test(s) reached the change
- [valkey-io/valkey-search#1002](https://github.com/valkey-io/valkey-search/pull/1002) ([tests](tests/valkey-search/1002-ef1f0a127/)): Rename utility-worker to util-worker to adhere to 15 char name limit in linux (#1002): declined: The change only renames the utility thread pool's name prefix from 'utility-worker-' to 'util-worker-' so that the string passed to pthread_setname_np fits Linux's 15-character thread-name limit. The name is used solely as an OS-level thread label for debugging (visible in /proc and top -H
- [valkey-io/valkey-search#960](https://github.com/valkey-io/valkey-search/pull/960) ([tests](tests/valkey-search/960-f1db9edb2/)): Fix FT.SEARCH bare wildcard execution (#960): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search#816](https://github.com/valkey-io/valkey-search/pull/816) ([tests](tests/valkey-search/816-7c217d43c/)): Refactor FT.AGGREGATE reducers to process records in batch (#816): 2 of 4 test(s) reached the change
- [valkey-io/valkey-search#777](https://github.com/valkey-io/valkey-search/pull/777) ([tests](tests/valkey-search/777-63574f75e/)): Fix build script for ASAN integration tests (#777): declined: The change modifies only build.sh and integration/run.sh in valkey-search's CI/test harness: it propagates pytest's exit code to the build script, captures pytest output to a log file, and scans that log for sanitizer errors in ASAN runs. No module source code, command behavior, replies, i
- [valkey-io/valkey-search#961](https://github.com/valkey-io/valkey-search/pull/961) ([tests](tests/valkey-search/961-153e037b4/)): fix test_dropindex_with_background_queries heap-use-after-free leak (#961): 0 of 2 test(s) reached the change; [crash, shown by ASan on unstable, the one build with it](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/78)
- [valkey-io/valkey-search#974](https://github.com/valkey-io/valkey-search/pull/974) ([tests](tests/valkey-search/974-b69c73a3f/)): Update search_number_of_indexes while restoring (#974): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search#966](https://github.com/valkey-io/valkey-search/pull/966) ([tests](tests/valkey-search/966-deab7ef9e/)): Fix crash on empty FT.AGGREGATE expressions (#966): 2 of 2 test(s) reached the change; crash, fixed by #966 (recorded)
- [valkey-io/valkey-search#973](https://github.com/valkey-io/valkey-search/pull/973) ([tests](tests/valkey-search/973-77ca3d550/)): fix aggregate recursion depth (#973): 0 of 4 test(s) reached the change
- [valkey-io/valkey-search#958](https://github.com/valkey-io/valkey-search/pull/958) ([tests](tests/valkey-search/958-541453bb0/)): Fix heap-use-after-free in PausePoint during server shutdown for mult… (#958): declined: no proposed test validated
- [valkey-io/valkey-search#956](https://github.com/valkey-io/valkey-search/pull/956) ([tests](tests/valkey-search/956-923430d2e/)): Fix dropindex while blocked queries crash (#956): 0 of 3 test(s) reached the change
- [valkey-io/valkey-search#943](https://github.com/valkey-io/valkey-search/pull/943) ([tests](tests/valkey-search/943-da472e234/)): Fix Boolean config type mismatch (#943): 0 of 1 test(s) reached the change

## Totals

- Merges whose tests reached the change: 290 of 399 tested (73%)
- Changed-line coverage: median 100% of 178 merge(s) measured, 8 under 50%
- Recall: 87 parents flagged of 122 fix merges
- Claude: $1752.89 in the last 24 hours, $2690.80 in the last 7 days, $2690.80 in all
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
