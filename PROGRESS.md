# Fuzzer walk

Updated 2026-11-07T19:24+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 729 | 1778 | #3197 (2026-02-23) | 323 | 104.1 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 481 | 188 | #362 (2025-09-09) | 200 | 68.7 |

## Last runs

### valkey-io/valkey

[2026-11-07T17:49:04+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42392779916): 20 merge(s) tested, 12 without a code change.

- [valkey-io/valkey#3303](https://github.com/valkey-io/valkey/pull/3303) ([tests](tests/valkey/3303-a731e45fd/)): Fix compatibility for OpenSSL < 3.0 and Almalinux version mismatch for daily tests (#3303): declined: The change is a compile-time compatibility fix plus CI config. The daily.yml edits only correct container names in the CI matrix. In src/tls.c, the preprocessor guards select SSL_get_peer_certificate()+X509_free() only when OPENSSL_VERSION_NUMBER < 0x30000000L; against OpenSSL 3.0+ (what t
- [valkey-io/valkey#3301](https://github.com/valkey-io/valkey/pull/3301) ([tests](tests/valkey/3301-896e9c871/)): Fix compatibility issue for death test with Valgrind (#3301): declined: The change modifies only the C++ unit-test infrastructure: an example death test in src/unit/example_tests.cpp now uses serverAssert(false) instead of a raw SEGV, and src/unit/main.cpp sets GoogleTest's death_test_style to 'threadsafe' when running under Valgrind. These files are part of t
- [valkey-io/valkey#3078](https://github.com/valkey-io/valkey/pull/3078) ([tests](tests/valkey/3078-7e110ae2b/)): Support TLS authentication using SAN URI (#3078): 2 of 4 test(s) reached the change, coverage 55%
- [valkey-io/valkey#3257](https://github.com/valkey-io/valkey/pull/3257) ([tests](tests/valkey/3257-b1f590f90/)): Fix TSAN compatibility for module loading (#3257): declined: The change is a sanitizer-compatibility build fix with no behaviour change on regular builds: it defines VALKEY_THREAD_SANITIZER and uses it only to (a) exclude RTLD_DEEPBIND from dlopen in moduleLoad when compiling under TSAN, mirroring the existing ASAN exclusion, and (b) mark the string
- [valkey-io/valkey#3278](https://github.com/valkey-io/valkey/pull/3278) ([tests](tests/valkey/3278-323ed0354/)): rename functions - prefix should be zset not zsl (#3278): declined: The change only renames two internal functions (zslParseLexRange -> zsetParseLexRange, zslFreeLexRange -> zsetFreeLexRange) and updates their declarations, call sites and comments. Every call site is updated identically with the same arguments and control flow; no command behavior, reply, 
- [valkey-io/valkey#2924](https://github.com/valkey-io/valkey/pull/2924) ([tests](tests/valkey/2924-293791100/)): Show replica dual-channel replication buffer memory in INFO MEMORY and MEMORY STATS (#2924): 2 of 5 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3283](https://github.com/valkey-io/valkey/pull/3283) ([tests](tests/valkey/3283-ec5e8fa2b/)): Fix use-after-free in generateSyncSlotsEstablishCommand (#3283): 0 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3281](https://github.com/valkey-io/valkey/pull/3281) ([tests](tests/valkey/3281-dab36efd3/)): Fix use zfree in cli --eval path  (#3281): 1 of 1 test(s) reached the change, coverage 100%; crash, fixed by #3281 (recorded)
- [valkey-io/valkey#3277](https://github.com/valkey-io/valkey/pull/3277) ([tests](tests/valkey/3277-74ad2f923/)): Suppress valgrind for BIO jobs (#3277): declined: The change is Valgrind test-tooling only: it removes the current_job field that existed solely to keep in-flight BIO jobs reachable for memcheck after thread cancellation, factors the identical zmalloc call into a noinline allocBioJob wrapper so its frame can be matched, and adds a matchin
- [valkey-io/valkey#3241](https://github.com/valkey-io/valkey/pull/3241) ([tests](tests/valkey/3241-f3e957cee/)): Introduce GoogleTest for Valkey unit testing (#3241): declined: The change migrates Valkey's unit tests from the in-house framework to GoogleTest: CI workflow updates, CMake/Makefile plumbing, vendored gtest-parallel, and test_*.c -> test_*.cpp rewrites. The only runtime-source edits are mechanical C++-compatibility changes with no behavioral effect: t
- [valkey-io/valkey#3072](https://github.com/valkey-io/valkey/pull/3072) ([tests](tests/valkey/3072-3a1e5d5ce/)): Lazy loading of RDMA libs in CLI/Benchmark when building as module (#3072): declined: Build-system-only change: it adds USE_DLOPEN_RDMA/ENABLE_DLOPEN_RDMA flags and drops RDMA_LIBS from the valkey-cli/valkey-benchmark link line when BUILD_RDMA=module, so the RDMA client library is dlopen'd lazily instead of being a hard link dependency. No source file changes; server comman
- [valkey-io/valkey#3256](https://github.com/valkey-io/valkey/pull/3256) ([tests](tests/valkey/3256-39332bdac/)): Replace mutexQueuePeek with current_job field for BIO memcheck fix (#3256): 0 of 10 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3255](https://github.com/valkey-io/valkey/pull/3255) ([tests](tests/valkey/3255-c47136491/)): Fix type of now parameter in rdbLoadObject() to use mstime_t instead of time_t (#3255): declined: The change only widens the type of rdbLoadObject()'s 'now' parameter from time_t to mstime_t. On the 64-bit Linux platform the fuzzer runs on, time_t and mstime_t are both 64-bit signed integers, so the parent build's implicit conversion of the mstime() value loses nothing and the single u
- [valkey-io/valkey#3029](https://github.com/valkey-io/valkey/pull/3029) ([tests](tests/valkey/3029-1ee434e63/)): Bugfix for GIL deadlock while unloading script engine, reenable memory test in crash report (#3029): declined: Both changes sit on paths the fuzzer cannot reach or observe. The GIL deadlock fix in scriptingEngineManagerUnregister is only invoked by VM_UnregisterScriptingEngine (unloading a module that registered a custom scripting engine while lazy-free jobs drain); this requires MODULE LOAD/UNLOAD
- [valkey-io/valkey#3175](https://github.com/valkey-io/valkey/pull/3175) ([tests](tests/valkey/3175-262b371da/)): Abort and swap the tables if ht1 is very full during the hashtable shrink rehashing (#3175): 3 of 4 test(s) reached the change, coverage 93%, 1 skipped (required capability 'build' is unsupported)
- [valkey-io/valkey#3091](https://github.com/valkey-io/valkey/pull/3091) ([tests](tests/valkey/3091-501b3153f/)): Skip expired hash fields when loading non-preamble RDB on primary (#3091): 3 of 5 test(s) reached the change, coverage 88%
- [valkey-io/valkey#3192](https://github.com/valkey-io/valkey/pull/3192) ([tests](tests/valkey/3192-537cb0794/)): Logging fix or improvement around new shard ID generation (#3192): 3 of 3 test(s) reached the change, coverage 91%; [check, introduced by #3192](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/98)
- [valkey-io/valkey@2565d445adf3](https://github.com/valkey-io/valkey/commit/2565d445adf3f256e7f124cc2387d9e15dd3bc39) ([tests](tests/valkey/2565d445a/)): Reset request type after handling empty requests: 2 of 5 test(s) reached the change, coverage 100%; crash, fixed by 2565d445adf3 (recorded)
- [valkey-io/valkey@0d713a4edf5c](https://github.com/valkey-io/valkey/commit/0d713a4edf5cab11e5edded2c089d1c09b1ed256) ([tests](tests/valkey/0d713a4ed/)): Fix for [CVE-2025-67733] RESP Protocol Injectiton via Lua error_reply: 0 of 2 test(s) reached the change, coverage 0%
- [valkey-io/valkey@61f990f1550c](https://github.com/valkey-io/valkey/commit/61f990f1550c6c69ad92ae3166d513bdccec0770) ([tests](tests/valkey/61f990f15/)): Fix for [CVE-2026-21863] Remote DoS with malformed Valkey Cluster bus message: declined: The fix hardens the cluster-bus binary parser (clusterIsValidPacket for PING/PONG/MEET) against malformed packets whose gossip count or EXT_DATA extension length exceeds the packet's totlen, causing an out-of-bounds read (CVE-2026-21863). The new rejection branches are reached only by deli

### valkey-io/valkey-bloom

[2026-11-07T19:21:40+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42405773185): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-07T19:21:35+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42405776550): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-07T19:24:49+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42394506825): 20 merge(s) tested, 3 without a code change.

- [valkey-io/valkey-search#494](https://github.com/valkey-io/valkey-search/pull/494) ([tests](tests/valkey-search/494-cdfe0c4b7/)): Fixed regression which broke Ninja builds OO (#494): declined: The change modifies only build.sh in valkey-search: it moves the Ninja/make build-tool selection after the function definitions and fixes ninja vs ninja-build detection so Ninja builds work again. It is purely a build-script/CI fix with no change to any module source, command behavior, rep
- [valkey-io/valkey-search#479](https://github.com/valkey-io/valkey-search/pull/479) ([tests](tests/valkey-search/479-e9f277cfd/)): Add support for checking ACL key prefix permissions using the new Valkey module API. (#479): 4 of 4 test(s) reached the change
- [valkey-io/valkey-search#405](https://github.com/valkey-io/valkey-search/pull/405) ([tests](tests/valkey-search/405-f683fffb3/)): Add more integration tests for FT.DROPINDEX consistency check (#405): 3 of 3 test(s) reached the change; crash, predates #405, since fixed (recorded)
- [valkey-io/valkey-search@b2aca8c691f3](https://github.com/valkey-io/valkey-search/commit/b2aca8c691f3a9d38098ab720268549de9d14ce0) ([tests](tests/valkey-search/b2aca8c69/)): Fix typo in ft.aggregate commit: declined: The change only fixes a typo in an error string inside the filter_str.empty() branch of the file-local ParseKNN (ft_search_parser.cc:185). That branch is unreachable: ParseKNN's sole caller, PreParseQueryString at ft_search_parser.cc:393, is guarded by !vector_filter.empty() on the very st
- [valkey-io/valkey-search#345](https://github.com/valkey-io/valkey-search/pull/345) ([tests](tests/valkey-search/345-b3b3449fb/)): Implementation of FT.AGGREGATE Command (#345): 4 of 4 test(s) reached the change
- [valkey-io/valkey-search#408](https://github.com/valkey-io/valkey-search/pull/408) ([tests](tests/valkey-search/408-0cd447cfc/)): Implement server version checking on load (#408): 0 of 3 test(s) reached the change
- [valkey-io/valkey-search#393](https://github.com/valkey-io/valkey-search/pull/393) ([tests](tests/valkey-search/393-73138472e/)): FT.DROPINDEX consistency check (#393): declined: The change is real behaviour (FT.DROPINDEX in cluster mode now fans out a consistency check over the coordinator's gRPC channel, retrying until every node reports the index gone; gRPC NOT_FOUND is reclassified as INDEX_NAME_ERROR; FT.INFO CLUSTER retries on index-name errors), but every ch
- [valkey-io/valkey-search#404](https://github.com/valkey-io/valkey-search/pull/404) ([tests](tests/valkey-search/404-5ee8c81d3/)): add .kiro folder to .gitignore; enable developers to use kiro while prevent uploading kiro steering files (#404): declined: The change adds a single line (`.kiro/`) to .gitignore so developers' Kiro steering files aren't committed. It touches no source code, build configuration, or runtime behavior of the valkey-search module; there is no command, reply, replication, or persistence path to exercise.
- [valkey-io/valkey-search#386](https://github.com/valkey-io/valkey-search/pull/386) ([tests](tests/valkey-search/386-2812344e7/)): FT.CREATE consistency check (#386): declined: The only behavioural change sits behind the coordinator, which this fuzzer cannot enable. FT.CREATE's new consistency-check fanout runs only when `ValkeySearch::Instance().IsCluster() && UsingCoordinator()` (src/commands/ft_create.cc:63-70); otherwise the command takes the unchanged else-b
- [valkey-io/valkey-search#384](https://github.com/valkey-io/valkey-search/pull/384) ([tests](tests/valkey-search/384-95c6fde85/)): Add --format option to build script (#384): declined: The change only modifies build.sh, adding a --format option that runs clang-format on the source tree as a developer convenience. It is a build/tooling change with no effect on the compiled module's runtime behavior, commands, replies, persistence, or replication, so there is nothing for a
- [valkey-io/valkey-search#337](https://github.com/valkey-io/valkey-search/pull/337) ([tests](tests/valkey-search/337-4be4f5680/)): Adding retry to fanout base class (#337): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search#390](https://github.com/valkey-io/valkey-search/pull/390) ([tests](tests/valkey-search/390-0e185efac/)): Captured errors should be formatted as such. (#390): declined: The change is confined to the unit-test infrastructure: vmsdk/src/testing_infra/module.h's ReplyCapture mock (used only by gtest) now formats captured errors as RESP errors ('-msg') instead of bulk strings, and testing/ft_create_test.cc, ft_info_test.cc and ft_search_test.cc update their e
- [valkey-io/valkey-search#395](https://github.com/valkey-io/valkey-search/pull/395) ([tests](tests/valkey-search/395-d3c87b5ce/)): tag casesentive updated to be key-value pair (#395): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#370](https://github.com/valkey-io/valkey-search/pull/370) ([tests](tests/valkey-search/370-af2e9a8e0/)): added and removed stats and attributes from ft.info (#370): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#388](https://github.com/valkey-io/valkey-search/pull/388) ([tests](tests/valkey-search/388-7cbef1e01/)): fix memory leak at config get (#388): declined: The change rewrites OnGetStringConfig, the CONFIG GET callback for string-typed module configs in vmsdk (caching the ValkeyModuleString instead of leaking a fresh one per call). That callback is only installed by config::String::Register, and valkey-search at this commit never instantiates
- [valkey-io/valkey-search#387](https://github.com/valkey-io/valkey-search/pull/387) ([tests](tests/valkey-search/387-7182af06d/)): Remove command alloed acl checks is it is already done by Valkey (#387): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#363](https://github.com/valkey-io/valkey-search/pull/363) ([tests](tests/valkey-search/363-329da6072/)): Fix valkey and json module versions in build and integration test. (#363): declined: The change edits only scripts/common.rc, a build/CI helper script that clones and builds valkey-server and the valkey-json module for valkey-search's own integration tests: it makes VALKEY_VERSION and VALKEY_JSON_VERSION overridable env vars and clones valkey-json by branch instead of a pi
- [valkey-io/valkey-search#382](https://github.com/valkey-io/valkey-search/pull/382) ([tests](tests/valkey-search/382-e8b166bfd/)): Early return mutation handling on record not found and not tracked (#382): 2 of 2 test(s) reached the change; crash, predates #382, since fixed (recorded)
- [valkey-io/valkey-search#381](https://github.com/valkey-io/valkey-search/pull/381) ([tests](tests/valkey-search/381-688cb371d/)): Fixed: clients are blocked after MULTI operation (#381): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#379](https://github.com/valkey-io/valkey-search/pull/379) ([tests](tests/valkey-search/379-90e550e13/)): - Remove unused variable from test (#379): declined: The change only removes unused variables and magic numbers from a unit test file (thread_pool_test.cc) and adds `inline` to two explicit template specializations in debug.h to fix ODR/linkage for a header-defined function. The function bodies are unchanged, so no command, reply, indexing, 

## Totals

- Merges whose tests reached the change: 473 of 690 tested (69%)
- Changed-line coverage: median 100% of 329 merge(s) measured, 21 under 50%
- Recall: 137 parents flagged of 204 fix merges
- Claude: $1867.51 in the last 24 hours, $4945.14 in the last 7 days, $4945.14 in all
- Issues filed: 50
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
