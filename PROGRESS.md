# Fuzzer walk

Updated 2026-11-07T18:50+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 729 | 1778 | #3197 (2026-02-23) | 323 | 104.1 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 458 | 211 | #405 (2025-10-14) | 177 | 65.4 |

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

[2026-11-07T18:50:53+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42403250939): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-07T18:50:54+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42403256358): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-07T17:04:51+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42377465148): 20 merge(s) tested, 5 without a code change.

- [valkey-io/valkey-search#494](https://github.com/valkey-io/valkey-search/pull/494): to retry, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey-search#479](https://github.com/valkey-io/valkey-search/pull/479): to retry, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey-search#401](https://github.com/valkey-io/valkey-search/pull/401) ([tests](tests/valkey-search/401-c2c1e7b95/)): Revise Save/Restore for true pit snapshot. (#401): 3 of 5 test(s) reached the change; check, not reproduced (recorded)
- [valkey-io/valkey-search#489](https://github.com/valkey-io/valkey-search/pull/489) ([tests](tests/valkey-search/489-0b8c620f2/)): Update ProcessKeyspaceNotification to not return before trying out all attributes (#489): 2 of 2 test(s) reached the change; crash, introduced and since fixed (recorded)
- [valkey-io/valkey-search#483](https://github.com/valkey-io/valkey-search/pull/483) ([tests](tests/valkey-search/483-19977d452/)): fix build.sh for cloud desktop (#483): declined: The change touches only build.sh, moving the BUILD_TOOL selection (ninja vs make) earlier so the script works on cloud desktop environments. It changes how the module is compiled, not any code that runs inside Valkey: no command, reply, index, persistence or replication behaviour differs b
- [valkey-io/valkey-search#481](https://github.com/valkey-io/valkey-search/pull/481) ([tests](tests/valkey-search/481-e8c6cd1f8/)): Fix cluster map bugs (#481): declined: Every changed line is gated behind the valkey-search coordinator: the new ClusterMap::GetTargets modes, the ParseNodeInfo is_local fix, and the kOneReplicaPerShard fanout are only exercised by FT.SEARCH/FT.INFO/FT.CREATE/FT.DROPINDEX fanout paths that run only when UsingCoordinator() is tr
- [valkey-io/valkey-search#477](https://github.com/valkey-io/valkey-search/pull/477) ([tests](tests/valkey-search/477-b77384b55/)): Replace multiple utility libraries with unified vmsdklib dependency (#477): declined: The change is a build-system refactor: it merges the many vmsdk static libraries into a single vmsdklib archive and rewrites CMake link lines, plus a CMAKE_GENERATOR option in build.sh. The only source edits are behavior-free: constant renames in vmsdk/src/cluster_map.cc/.h (VALKEY_MODULE_
- [valkey-io/valkey-search#475](https://github.com/valkey-io/valkey-search/pull/475) ([tests](tests/valkey-search/475-6602e5cdf/)): Add output files from original integration tests to github upload (#475): declined: The change modifies only ci/build_ubuntu.sh, adding cp commands so that the integration tests' output and tmp directories are included in the CI artifact upload. It is purely a CI/build-script change: no module code, command behaviour, replies, persistence, or replication paths are affecte
- [valkey-io/valkey-search#476](https://github.com/valkey-io/valkey-search/pull/476) ([tests](tests/valkey-search/476-837f7d32e/)): fix crash on aof (#476): declined: Every behavioral change in this diff is gated behind `ValkeySearch::Instance().IsCluster() && ValkeySearch::Instance().UsingCoordinator()`. In ft_create.cc and ft_dropindex.cc the fix only alters the `IsCluster && UsingCoordinator` branch (adding `!is_loading && !inside_multi_exec`); with 
- [valkey-io/valkey-search#466](https://github.com/valkey-io/valkey-search/pull/466) ([tests](tests/valkey-search/466-65f6ed48e/)): FT.INFO parser (#466): 3 of 3 test(s) reached the change; crash, predates #466, since fixed (recorded)
- [valkey-io/valkey-search@b9ddc9c81bc0](https://github.com/valkey-io/valkey-search/commit/b9ddc9c81bc01f0561e8a3360f8261568edbad08) ([tests](tests/valkey-search/b9ddc9c81/)): Fix bad merge on compatibility: 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#458](https://github.com/valkey-io/valkey-search/pull/458) ([tests](tests/valkey-search/458-d7df48f07/)): change FanoutSearchTargets to NodeInfo; fix fanout_test; add cancelled check in Reply() (#458): 0 of 2 test(s) reached the change; crash, predates #458, since fixed (recorded)
- [valkey-io/valkey-search#446](https://github.com/valkey-io/valkey-search/pull/446) ([tests](tests/valkey-search/446-e320a56bb/)): Aggregate Cleanup (#446): 2 of 2 test(s) reached the change; crash, predates #446, since fixed (recorded)
- [valkey-io/valkey-search#435](https://github.com/valkey-io/valkey-search/pull/435) ([tests](tests/valkey-search/435-c71e26a83/)): Cluster Map Implementation (#435): declined: no proposed test validated
- [valkey-io/valkey-search#453](https://github.com/valkey-io/valkey-search/pull/453) ([tests](tests/valkey-search/453-a3aeb3775/)): Fix build script to work when directory path contains spaces (#453): declined: The change only quotes paths and restructures argument passing in build.sh, scripts/common.rc and submodules/CMakeLists.txt so the build works when the directory path contains spaces. It touches no module source code, no command handling, no replies, no persistence or replication path: the
- [valkey-io/valkey-search#427](https://github.com/valkey-io/valkey-search/pull/427) ([tests](tests/valkey-search/427-91fab2971/)): Prevent crash on Valkey 7.2 when loading valkey-search (#427): declined: The change only alters module load-time behaviour on Valkey servers older than 8.1.1 (e.g. 7.2): it moves the minimum-version check before the calls to ACL/command-registration APIs that don't exist there, and guards against ValkeyModule_GetServerVersion being null, so MODULE LOAD fails cl
- [valkey-io/valkey-search#426](https://github.com/valkey-io/valkey-search/pull/426) ([tests](tests/valkey-search/426-db0089e99/)): Enable upload of integration test outputs (#426): declined: The change modifies only CI workflow files (.github/workflows/*), build.sh argument parsing, and ci/build_ubuntu.sh to upload unit/integration test outputs as GitHub artifacts and enable core dumps in CI containers. No source file of the valkey-search module is touched, so the loaded modul
- [valkey-io/valkey-search#415](https://github.com/valkey-io/valkey-search/pull/415) ([tests](tests/valkey-search/415-090c082a8/)): prevent Valkey crash when module is loaded twice (#415): declined: The change is purely load-time: a static guard at the top of valkey-search's ValkeyModule_OnLoad that returns VALKEYMODULE_ERR instead of letting TrackCurrentAsMainThread's CHECK abort when the same libsearch.so is loaded a second time in one process. It alters no command reply, replicatio
- [valkey-io/valkey-search#419](https://github.com/valkey-io/valkey-search/pull/419) ([tests](tests/valkey-search/419-f998b7959/)): Add inline keyword to ostream operator overloads for better linking on macOS and build compatibility. (#419): declined: The change only adds the inline keyword to three ostream operator<< overloads defined in headers (fixing duplicate-symbol link errors on macOS) and adds a macOS CI workflow file. Adding inline to header-defined functions changes linkage, not semantics: the compiled module behaves identical
- [valkey-io/valkey-search#400](https://github.com/valkey-io/valkey-search/pull/400) ([tests](tests/valkey-search/400-a3148e1d9/)): addressing non-vector search issues (#400): 2 of 2 test(s) reached the change

## Totals

- Merges whose tests reached the change: 464 of 680 tested (68%)
- Changed-line coverage: median 100% of 329 merge(s) measured, 21 under 50%
- Recall: 136 parents flagged of 203 fix merges
- Claude: $1788.97 in the last 24 hours, $4866.60 in the last 7 days, $4866.60 in all
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

- [valkey-io/valkey-search#494](https://github.com/valkey-io/valkey-search/pull/494): attempt 2 of 3, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey-search#479](https://github.com/valkey-io/valkey-search/pull/479): attempt 2 of 3, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey#3800](https://github.com/valkey-io/valkey/pull/3800): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#3613](https://github.com/valkey-io/valkey/pull/3613): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#3498](https://github.com/valkey-io/valkey/pull/3498): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
