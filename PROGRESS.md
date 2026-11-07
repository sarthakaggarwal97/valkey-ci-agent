# Fuzzer walk

Updated 2026-11-07T16:44+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 697 | 1810 | #3303 (2026-03-05) | 316 | 99.6 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 435 | 234 | #401 (2025-11-21) | 180 | 62.1 |

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

[2026-11-07T16:36:28+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42392094941): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-07T16:36:16+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42392077108): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-07T13:32:51+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42369561194): 20 merge(s) tested, 2 without a code change.

- [valkey-io/valkey-search#536](https://github.com/valkey-io/valkey-search/pull/536) ([tests](tests/valkey-search/536-f06b8b2ca/)): ignore venv (#536): declined: The change only adds venv/, env/, .venv/ and .env/ entries to .gitignore. It is a repository housekeeping change with no code, build, or runtime behaviour change in the valkey-search module, so there is nothing a scenario could exercise or compare.
- [valkey-io/valkey-search#533](https://github.com/valkey-io/valkey-search/pull/533) ([tests](tests/valkey-search/533-3f444fae0/)): Feature/ft create sortable (#533): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#531](https://github.com/valkey-io/valkey-search/pull/531) ([tests](tests/valkey-search/531-4da6e969e/)): refresh cluster map in background (#531): declined: The change adds one branch in ValkeySearch::OnServerCronCallback that refreshes the cluster map in the background, gated on IsCluster() && UsingCoordinator(). UsingCoordinator() is true only when the hidden, startup-only 'use-coordinator' config is set, and the only mechanism that sets it 
- [valkey-io/valkey-search#528](https://github.com/valkey-io/valkey-search/pull/528) ([tests](tests/valkey-search/528-c3fa174e8/)): Change default Valkey to 9.0.1 release (#528): declined: The change is a CI version bump (scripts/common.rc, testing/integration/run.sh set default Valkey to 9.0.1) plus raising kMinimumServerVersion in src/version.h from 8.1.1 to 9.0.1. That constant is read only once, in vmsdk/src/module.cc's load-time gate: if the server version is below it, 
- [valkey-io/valkey-search#527](https://github.com/valkey-io/valkey-search/pull/527) ([tests](tests/valkey-search/527-ed0648404/)): Fix Aggregation processing of LOAD clause and validation of vector query parameters. (#527): 2 of 2 test(s) reached the change; crash, predates #527, since fixed (recorded)
- [valkey-io/valkey-search#524](https://github.com/valkey-io/valkey-search/pull/524) ([tests](tests/valkey-search/524-31d37d91d/)): use negate flag parser in ft.info parser (#524): declined: Pure refactor with no behaviour change: the diff replaces two inline lambda parsers for FT.INFO's ALLSHARDS and INCONSISTENT keywords with the GENERATE_NEGATED_FLAG_PARSER macro, which expands (vmsdk/src/command_parser.h:211-216) to an identical lambda that sets the same field (enable_part
- [valkey-io/valkey-search#510](https://github.com/valkey-io/valkey-search/pull/510) ([tests](tests/valkey-search/510-990efa0ee/)): Fix replica crash by using legacy ACL path during replication (#510): 0 of 1 test(s) reached the change
- [valkey-io/valkey-search#410](https://github.com/valkey-io/valkey-search/pull/410) ([tests](tests/valkey-search/410-fd9d5ea4b/)): Full support for DB numbers other than zero in cluster mode. (#410): 4 of 4 test(s) reached the change; crash, introduced and since fixed (recorded); crash, fixed by #410 (recorded); crash, fixed by #410 (recorded); crash, fixed by #410 (recorded)
- [valkey-io/valkey-search#522](https://github.com/valkey-io/valkey-search/pull/522) ([tests](tests/valkey-search/522-6706ea268/)): Limit and Nocontent support for non vector queries (#522): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#469](https://github.com/valkey-io/valkey-search/pull/469) ([tests](tests/valkey-search/469-b81574a20/)): FT.INFO partition and consistency controls (#469): 3 of 3 test(s) reached the change; crash, predates #469, since fixed (recorded)
- [valkey-io/valkey-search#464](https://github.com/valkey-io/valkey-search/pull/464) ([tests](tests/valkey-search/464-0026dfcf1/)): FT.SEARCH partition and consistency controls (#464): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#299](https://github.com/valkey-io/valkey-search/pull/299) ([tests](tests/valkey-search/299-c50f8413a/)): feat: set command info for ft.create (#299): 2 of 2 test(s) reached the change; crash, introduced and since fixed (recorded); crash, fixed by #299 (recorded); crash, fixed by #299 (recorded)
- [valkey-io/valkey-search#496](https://github.com/valkey-io/valkey-search/pull/496) ([tests](tests/valkey-search/496-de6457112/)): Fix Override Specifier and Suppress MacOS Compiler Warning (#496): declined: The change modifies only CMakeLists.txt: it reformats the SAN_BUILD cache variable and sanitizer messages, and adds -Wno-defaulted-function-deleted on Apple builds to suppress a macOS compiler warning. The diff is complete (diff_truncated: false) and contains no C++ source changes, so no r
- [valkey-io/valkey-search#503](https://github.com/valkey-io/valkey-search/pull/503) ([tests](tests/valkey-search/503-d09a78fc9/)): Fix warnings and formatting (#503): declined: The change is a warnings-and-formatting cleanup with no behaviour change. The only production-code edit adds `default: CHECK(false)` to the KeyAccessToStringView switch in src/acl.cc, which is unreachable: acl::KeyAccess has exactly two enumerators (kRead, kWrite) and every call site passe
- [valkey-io/valkey-search#480](https://github.com/valkey-io/valkey-search/pull/480) ([tests](tests/valkey-search/480-57e5dfde0/)): Populate fingerprint and version to IndexSchema (#480): declined: Every behavioral change sits behind the coordinator, which only the module load-time argument --use-coordinator can enable: the 'use-coordinator' config is built Hidden (valkey_search_options.cc:129) and ModuleConfigManager::Init skips hidden entries, so it is not registered with Valkey's 
- [valkey-io/valkey-search#428](https://github.com/valkey-io/valkey-search/pull/428) ([tests](tests/valkey-search/428-cad62657f/)): Implement queue wait time aware local node preference in fanout operations (#428): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#495](https://github.com/valkey-io/valkey-search/pull/495) ([tests](tests/valkey-search/495-be8b2368f/)): Fix flaky save/restore test (#495): declined: The change is a test fix plus diagnostics: the integration test gains a wait for index backfill after restart (test code only), log.cc loses a blank line, and module OnLoad gains a read-only loop over /proc/self/maps that logs each loaded .so's base address at NOTICE level to make addr2lin
- [valkey-io/valkey-search#494](https://github.com/valkey-io/valkey-search/pull/494): to retry, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey-search#479](https://github.com/valkey-io/valkey-search/pull/479): to retry, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey-search#492](https://github.com/valkey-io/valkey-search/pull/492) ([tests](tests/valkey-search/492-d90a35802/)): fix mem leak in unit test (#492): declined: The change touches only testing/valkey_search_test.cc, the module's own gtest unit test: it replaces a raw heap allocation of a mock CLUSTER SLOTS reply with a std::unique_ptr so the mock reply is freed when the test doesn't delete it, fixing a leak in the test harness itself. No shipped m

## Totals

- Merges whose tests reached the change: 451 of 663 tested (68%)
- Changed-line coverage: median 100% of 319 merge(s) measured, 20 under 50%
- Recall: 132 parents flagged of 196 fix merges
- Claude: $1821.67 in the last 24 hours, $4723.25 in the last 7 days, $4723.25 in all
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

- [valkey-io/valkey-search#494](https://github.com/valkey-io/valkey-search/pull/494): attempt 1 of 3, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey-search#479](https://github.com/valkey-io/valkey-search/pull/479): attempt 1 of 3, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey#3800](https://github.com/valkey-io/valkey/pull/3800): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#3613](https://github.com/valkey-io/valkey/pull/3613): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#3498](https://github.com/valkey-io/valkey/pull/3498): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
