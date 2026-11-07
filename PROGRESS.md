# Fuzzer walk

Updated 2026-11-07T17:37+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 697 | 1810 | #3303 (2026-03-05) | 316 | 99.6 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 458 | 211 | #405 (2025-10-14) | 177 | 65.4 |

## Last runs

### valkey-io/valkey

[2026-11-07T16:44:05+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42388835142): 20 merge(s) tested, 7 without a code change.

- [valkey-io/valkey#3421](https://github.com/valkey-io/valkey/pull/3421) ([tests](tests/valkey/3421-2cde059da/)): Handle EAGAIN in clusterWriteHandler (#3421): 0 of 6 test(s) reached the change, coverage 0%, 1 skipped (required capability 'save-behavior-support' is unsupported)
- [valkey-io/valkey#3352](https://github.com/valkey-io/valkey/pull/3352) ([tests](tests/valkey/3352-26522fc52/)): Add availability-zone to the reply schema for CLUSTER SHARDS/SLOTS (#3352): 2 of 2 test(s) reached the change; [check, appeared after #3352](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/95); check, introduced and since fixed (recorded); [check, appeared after #3352](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/96); check, introduced and since fixed (recorded)
- [valkey-io/valkey#1546](https://github.com/valkey-io/valkey/pull/1546) ([tests](tests/valkey/1546-7403b423c/)): Add unsigned 64-bit numeric config in module API (#1546): 2 of 2 test(s) reached the change, coverage 97%
- [valkey-io/valkey#3329](https://github.com/valkey-io/valkey/pull/3329) ([tests](tests/valkey/3329-641472050/)): Replace fast_float (C++) with ffc.h (#3329): 3 of 3 test(s) reached the change, coverage 94%
- [valkey-io/valkey#3344](https://github.com/valkey-io/valkey/pull/3344) ([tests](tests/valkey/3344-8051de740/)): Inherit LDFLAGS for TLS and RDMA modules (#3344): declined: The change is build-only: it adds $(LDFLAGS) to the Makefile link lines for the optional valkey-tls.so and valkey-rdma.so modules so externally supplied linker flags are inherited when building those shared objects. No source code, command behavior, reply, replication, persistence, or conf
- [valkey-io/valkey#3279](https://github.com/valkey-io/valkey/pull/3279) ([tests](tests/valkey/3279-a7d919a5b/)): Optimize SET key value EX/PX/EXAT ttl to reduce calls of rewriteClientCommandVector (#3279): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3342](https://github.com/valkey-io/valkey/pull/3342) ([tests](tests/valkey/3342-6b384e9a5/)): Fix ZDIFF algorithm 2 memory leak on early exit (#3342): 0 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3156](https://github.com/valkey-io/valkey/pull/3156) ([tests](tests/valkey/3156-374150355/)): Add availability-zone field support to CLUSTER SHARDS / CLUSTER SLOTS (#3156): 3 of 4 test(s) reached the change, coverage 87%; crash, intended by #3156: a panic the change adds (recorded)
- [valkey-io/valkey#3121](https://github.com/valkey-io/valkey/pull/3121) ([tests](tests/valkey/3121-ba5854636/)): New MSETEX command allows to set an expiration time with MSET (#3121): 2 of 5 test(s) reached the change, coverage 83%; check, introduced and since fixed (recorded)
- [valkey-io/valkey#3331](https://github.com/valkey-io/valkey/pull/3331) ([tests](tests/valkey/3331-1e2ca5ec5/)): fix alignment different between c and c++ in gtest (#3331): declined: The change rewrites _Atomic declarations in server.h from the qualifier form (_Atomic T) to the type-specifier form (_Atomic(T)), which are semantically identical in C, so the server binary's behavior is unchanged. The substantive edit is to src/unit/wrappers.h, a header used only when com
- [valkey-io/valkey#3258](https://github.com/valkey-io/valkey/pull/3258) ([tests](tests/valkey/3258-6fee1aa61/)): Use dictGetSomeKeys instead of dictGetRandomKey for gossip node population (#3258): 0 of 3 test(s) reached the change, coverage 87%, 1 skipped (required capability 'save-behavior-support' is unsupported)
- [valkey-io/valkey#2746](https://github.com/valkey-io/valkey/pull/2746) ([tests](tests/valkey/2746-0ff55de21/)): Make the number of healthy nodes in gossip section configurable (#2746): 1 of 4 test(s) reached the change, coverage 100%, 1 skipped (required capability 'save-behavior-support' is unsupported)
- [valkey-io/valkey#3252](https://github.com/valkey-io/valkey/pull/3252) ([tests](tests/valkey/3252-3d8a1b55e/)): Replace long long timestamp references with more specific mstime_t/ustime_t (#3252): declined: The change only renames types: every edit replaces a `long long` declaration, cast or sizeof with `mstime_t` or `ustime_t`, both of which are `typedef long long` (src/util.h:71-72). Type sizes, alignment, signedness, arithmetic, the entry expiry memory layout and all format strings are unc
- [valkey-io/valkey#3325](https://github.com/valkey-io/valkey/pull/3325) ([tests](tests/valkey/3325-9f0106755/)): Changed LUA module dependency (#3325): declined: The change touches only build files (deps/lua/src/Makefile, src/Makefile, src/modules/lua/Makefile): it reorders the engine_lua.so build dependency so the Lua module is built as a prerequisite of valkey-server rather than after it, filters the module out of the server link inputs, and adds
- [valkey-io/valkey#2934](https://github.com/valkey-io/valkey/pull/2934) ([tests](tests/valkey/2934-8a4a7b25d/)): CLUSTERSCAN Command (#2934): 4 of 6 test(s) reached the change
- [valkey-io/valkey#3280](https://github.com/valkey-io/valkey/pull/3280) ([tests](tests/valkey/3280-fc217fca2/)): Update cluster config file when NOFAILOVER changes (#3280): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3314](https://github.com/valkey-io/valkey/pull/3314) ([tests](tests/valkey/3314-8c397ea16/)): Fixed point arithmetic for x86 TSC (#3314): 1 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3311](https://github.com/valkey-io/valkey/pull/3311) ([tests](tests/valkey/3311-acb0b9b73/)): Recommend passwordless users for mTLS certificate-based authentication (#3311): declined: The change modifies only a Tcl test file (tests/unit/tls.tcl) and documentation comments in valkey.conf, rewording the tls-auth-clients-user docs to recommend passwordless users for mTLS certificate authentication. No server code, command behavior, config semantics, or protocol handling ch
- [valkey-io/valkey#3307](https://github.com/valkey-io/valkey/pull/3307) ([tests](tests/valkey/3307-cb9e7c94e/)): Let script continue if busy-reply-threshold is zero (#3307): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3316](https://github.com/valkey-io/valkey/pull/3316) ([tests](tests/valkey/3316-dcfb9269f/)): Fix zset to operate defrag scan lexicographically (#3316): 0 of 2 test(s) reached the change, coverage 100%; crash, fixed by #3316 (recorded)

### valkey-io/valkey-bloom

[2026-11-07T17:37:51+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42397209127): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-07T17:37:55+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42397183155): 0 merge(s) tested, 0 without a code change.


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

- Merges whose tests reached the change: 457 of 670 tested (68%)
- Changed-line coverage: median 100% of 319 merge(s) measured, 20 under 50%
- Recall: 134 parents flagged of 198 fix merges
- Claude: $1789.20 in the last 24 hours, $4802.46 in the last 7 days, $4802.46 in all
- Issues filed: 49
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

## Retries

- [valkey-io/valkey-search#494](https://github.com/valkey-io/valkey-search/pull/494): attempt 2 of 3, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey-search#479](https://github.com/valkey-io/valkey-search/pull/479): attempt 2 of 3, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey#3800](https://github.com/valkey-io/valkey/pull/3800): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#3613](https://github.com/valkey-io/valkey/pull/3613): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#3498](https://github.com/valkey-io/valkey/pull/3498): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
