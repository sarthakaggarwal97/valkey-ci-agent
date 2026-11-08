# Fuzzer walk

Updated 2026-11-08T04:33+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 840 | 1667 | #2964 (2026-01-05) | 328 | 120.0 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 563 | 106 | #152 (2025-05-29) | 212 | 80.4 |

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

[2026-11-08T04:31:59+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42447153218): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-08T04:23:32+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42446587061): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-08T04:33:16+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42441524138): 20 merge(s) tested, 2 without a code change.

- [valkey-io/valkey-search#207](https://github.com/valkey-io/valkey-search/pull/207) ([tests](tests/valkey-search/207-b6f344a85/)): Add FT._DEBUG command (#207): 3 of 3 test(s) reached the change; [crash, appeared after #207](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/87)
- [valkey-io/valkey-search#202](https://github.com/valkey-io/valkey-search/pull/202) ([tests](tests/valkey-search/202-71e80207c/)): Revision of info framework. (#202): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search#194](https://github.com/valkey-io/valkey-search/pull/194) ([tests](tests/valkey-search/194-b91e54993/)): Rebranding module to use Valkey instead of Redis (#194): 0 of 3 test(s) reached the change
- [valkey-io/valkey-search#198](https://github.com/valkey-io/valkey-search/pull/198) ([tests](tests/valkey-search/198-08b14a76f/)): Load JSON module into the test framework (#198): declined: The change is confined to valkey-search's own test/CI infrastructure: integration/run.sh gains a venv setup and a step that clones and builds the valkey-json module, the pytest fixture loads that JSON module next to the search module, and scripts/common.rc gets a log helper. No source file
- [valkey-io/valkey-search#173](https://github.com/valkey-io/valkey-search/pull/173) ([tests](tests/valkey-search/173-254b2cb53/)): Spelling (#173): declined: The change is a spelling-only PR: it fixes typos in documentation (COMMANDS.md, README.md, QUICK_START.md, rfc/rdb-format.md), code comments, and build scripts, and renames internal identifiers consistently at declaration and every use site in the same diff (CalcBestMatchingPrefiltereddKey
- [valkey-io/valkey-search#188](https://github.com/valkey-io/valkey-search/pull/188) ([tests](tests/valkey-search/188-03b5b06a3/)): Integrate valkey-test-framework (#188): declined: The change only adds integration-test infrastructure to the valkey-search repository: a new Python test harness (integration/valkey_search_test_case.py, integration/test_vss_basic.py), a test runner script (integration/run.sh), a README, and shell helpers (scripts/common.rc). No file under
- [valkey-io/valkey-search#192](https://github.com/valkey-io/valkey-search/pull/192) ([tests](tests/valkey-search/192-cc56d7979/)): Fix broken CI (#192): declined: The change modifies only CI infrastructure scripts: ci/build_ubuntu.sh (removes wget --show-progress, affecting only CI log verbosity) and testing/integration/run.sh (pins the valkey-json dependency to a fixed commit and builds it with --release for the integration test harness). No module
- [valkey-io/valkey-search#12](https://github.com/valkey-io/valkey-search/pull/12) ([tests](tests/valkey-search/12-403d7b574/)): Initial text search RFC (#12): declined: The change adds only a documentation file (rfc/text-field.md, an RFC proposing a future TEXT field type) and a .vscode editor settings tweak. No source code, build file, command, or configuration of the module is modified, so there is no behaviour change to test.
- [valkey-io/valkey-search#178](https://github.com/valkey-io/valkey-search/pull/178) ([tests](tests/valkey-search/178-09411fb98/)): The module can now be built on macOS (#178): declined: The change is a macOS build-portability PR: almost all hunks are CMake/build.sh changes, macOS-only submodule patches, or test-only code. The three runtime hunks are no-ops on the Linux/gcc builds the fuzzer runs: the filter_parser infinity constants only change under #if defined(__clang__
- [valkey-io/valkey-search#177](https://github.com/valkey-io/valkey-search/pull/177) ([tests](tests/valkey-search/177-221d4c602/)): Add coordinator port info metric (#177): declined: no proposed test validated
- [valkey-io/valkey-search#172](https://github.com/valkey-io/valkey-search/pull/172): to retry, `export NINJA_TOOL=ninja` failed in parent (exit 1): [22/206] Building C object third_party/hdrhistogram_c/src/CMakeFiles/hdrhistogram_c.dir/hdr_encoding.c.o [23/206] Building C object third_party/hdr
- [valkey-io/valkey-search#144](https://github.com/valkey-io/valkey-search/pull/144): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): [20/206] Linking CXX static library vmsdk/src/libmemory_allocation_overrides.a [21/206] Building C object third_party/hdrhistogram_c/src/CMakeFiles/
- [valkey-io/valkey-search#168](https://github.com/valkey-io/valkey-search/pull/168): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): [10/206] Running cpp protocol buffer compiler on /home/runner/work/valkey-ci-agent/valkey-ci-agent/walk-work/merge/third_party/hnswlib/index.proto [
- [valkey-io/valkey-search#166](https://github.com/valkey-io/valkey-search/pull/166): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): [15/206] Linking CXX static library vmsdk/src/status/libstatus_macros.a [16/206] Building CXX object vmsdk/src/CMakeFiles/log.dir/log.cc.o [17/206] 
- [valkey-io/valkey-search#162](https://github.com/valkey-io/valkey-search/pull/162): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): [22/206] Building CXX object third_party/hnswlib/CMakeFiles/index_cc_proto.dir/index.pb.cc.o [23/206] Building C object third_party/hdrhistogram_c/s
- [valkey-io/valkey-search#161](https://github.com/valkey-io/valkey-search/pull/161): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): [21/206] Building C object third_party/hdrhistogram_c/src/CMakeFiles/hdrhistogram_c.dir/hdr_encoding.c.o [22/206] Building CXX object src/CMakeFiles
- [valkey-io/valkey-search#159](https://github.com/valkey-io/valkey-search/pull/159): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): [20/206] Linking CXX static library src/libattribute_data_type.a [21/206] Linking CXX static library vmsdk/src/libmemory_allocation_overrides.a [22/
- [valkey-io/valkey-search#158](https://github.com/valkey-io/valkey-search/pull/158): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): [13/206] Linking CXX static library vmsdk/src/libmemory_allocation.a [14/206] Building CXX object src/indexes/CMakeFiles/index_schema_cc_proto.dir/_
- [valkey-io/valkey-search#148](https://github.com/valkey-io/valkey-search/pull/148): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): [15/206] Linking CXX static library src/indexes/libindex_schema_cc_proto.a [16/206] Building CXX object vmsdk/src/CMakeFiles/log.dir/log.cc.o [17/20
- [valkey-io/valkey-search#154](https://github.com/valkey-io/valkey-search/pull/154): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): [20/206] Building CXX object third_party/hnswlib/CMakeFiles/index_cc_proto.dir/index.pb.cc.o [21/206] Building CXX object vmsdk/src/CMakeFiles/memor

## Totals

- Merges whose tests reached the change: 546 of 788 tested (69%)
- Changed-line coverage: median 100% of 378 merge(s) measured, 24 under 50%
- Recall: 153 parents flagged of 225 fix merges
- Claude: $1925.28 in the last 24 hours, $5633.43 in the last 7 days, $5633.43 in all
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

- [valkey-io/valkey-search#172](https://github.com/valkey-io/valkey-search/pull/172): attempt 1 of 3, `export NINJA_TOOL=ninja` failed in parent (exit 1): [22/206] Building C object third_party/hdrhistogram_c/src/CMakeFiles/hdrhistogram_c.dir/hdr_encoding.c.o [23/206] Building C object third_party/hdr
- [valkey-io/valkey-search#144](https://github.com/valkey-io/valkey-search/pull/144): attempt 1 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): [20/206] Linking CXX static library vmsdk/src/libmemory_allocation_overrides.a [21/206] Building C object third_party/hdrhistogram_c/src/CMakeFiles/
- [valkey-io/valkey-search#168](https://github.com/valkey-io/valkey-search/pull/168): attempt 1 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): [10/206] Running cpp protocol buffer compiler on /home/runner/work/valkey-ci-agent/valkey-ci-agent/walk-work/merge/third_party/hnswlib/index.proto [
- [valkey-io/valkey-search#166](https://github.com/valkey-io/valkey-search/pull/166): attempt 1 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): [15/206] Linking CXX static library vmsdk/src/status/libstatus_macros.a [16/206] Building CXX object vmsdk/src/CMakeFiles/log.dir/log.cc.o [17/206] 
- [valkey-io/valkey-search#162](https://github.com/valkey-io/valkey-search/pull/162): attempt 1 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): [22/206] Building CXX object third_party/hnswlib/CMakeFiles/index_cc_proto.dir/index.pb.cc.o [23/206] Building C object third_party/hdrhistogram_c/s
- [valkey-io/valkey-search#161](https://github.com/valkey-io/valkey-search/pull/161): attempt 1 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): [21/206] Building C object third_party/hdrhistogram_c/src/CMakeFiles/hdrhistogram_c.dir/hdr_encoding.c.o [22/206] Building CXX object src/CMakeFiles
- [valkey-io/valkey-search#159](https://github.com/valkey-io/valkey-search/pull/159): attempt 1 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): [20/206] Linking CXX static library src/libattribute_data_type.a [21/206] Linking CXX static library vmsdk/src/libmemory_allocation_overrides.a [22/
- [valkey-io/valkey-search#158](https://github.com/valkey-io/valkey-search/pull/158): attempt 1 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): [13/206] Linking CXX static library vmsdk/src/libmemory_allocation.a [14/206] Building CXX object src/indexes/CMakeFiles/index_schema_cc_proto.dir/_
- [valkey-io/valkey-search#148](https://github.com/valkey-io/valkey-search/pull/148): attempt 1 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): [15/206] Linking CXX static library src/indexes/libindex_schema_cc_proto.a [16/206] Building CXX object vmsdk/src/CMakeFiles/log.dir/log.cc.o [17/20
- [valkey-io/valkey-search#154](https://github.com/valkey-io/valkey-search/pull/154): attempt 1 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): [20/206] Building CXX object third_party/hnswlib/CMakeFiles/index_cc_proto.dir/index.pb.cc.o [21/206] Building CXX object vmsdk/src/CMakeFiles/memor
- [valkey-io/valkey#3800](https://github.com/valkey-io/valkey/pull/3800): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#3613](https://github.com/valkey-io/valkey/pull/3613): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#3498](https://github.com/valkey-io/valkey/pull/3498): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
