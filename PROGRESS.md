# Fuzzer walk

Updated 2026-11-08T03:00+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 840 | 1667 | #2964 (2026-01-05) | 328 | 120.0 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 530 | 139 | #218 (2025-07-21) | 179 | 75.7 |

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

[2026-11-08T03:00:00+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42440785571): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-08T02:51:28+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42440233228): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-08T00:37:24+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42417851138): 20 merge(s) tested, 5 without a code change.

- [valkey-io/valkey-search#308](https://github.com/valkey-io/valkey-search/pull/308) ([tests](tests/valkey-search/308-cb39bf3f7/)): Strip the complete build path from logs (#308): declined: The change is build-only: it adds -ffile-prefix-map=${CMAKE_SOURCE_DIR}= to the module's compile flags in cmake/Modules/valkey_search.cmake, which only strips the absolute build directory from __FILE__ strings and debug info used in log messages and backtraces. No command behaviour, reply,
- [valkey-io/valkey-search#319](https://github.com/valkey-io/valkey-search/pull/319) ([tests](tests/valkey-search/319-a90471352/)): Update 3 fields returned type of FT.INFO (#319): 2 of 2 test(s) reached the change; crash, predates #319, since fixed (recorded)
- [valkey-io/valkey-search#310](https://github.com/valkey-io/valkey-search/pull/310) ([tests](tests/valkey-search/310-3d27b2cdb/)): Fanout operation base and cluster-wide primary and cluster fanout operation modes (#310): 2 of 2 test(s) reached the change; crash, predates #310, since fixed (recorded)
- [valkey-io/valkey-search#313](https://github.com/valkey-io/valkey-search/pull/313) ([tests](tests/valkey-search/313-1b71811d4/)): Added support for cluster with replicas (#313): declined: The change modifies only the project's CI script (build.sh) and its Python integration-test framework (integration/valkey_search_test_case.py and test files): it adds replica support to the test harness itself — new Node/ReplicationGroup helpers, parametrized fixtures, a renamed temp direc
- [valkey-io/valkey-search#290](https://github.com/valkey-io/valkey-search/pull/290) ([tests](tests/valkey-search/290-7b759eb68/)): Add gc_stats, cursor_stats, dialect_stats, Index Errors field part for FT.INFO returned fields (#290): 2 of 2 test(s) reached the change; crash, predates #290, since fixed (recorded)
- [valkey-io/valkey-search#307](https://github.com/valkey-io/valkey-search/pull/307) ([tests](tests/valkey-search/307-3b82b53e6/)): Build script fixes (#307): declined: The change modifies only build/CI scripts (build.sh, integration/run.sh, and the GitHub Actions workflow) to add a --retries=N option for the project's own integration test runner. No module source code is touched, so the compiled valkey-search module is behaviourally identical at the chan
- [valkey-io/valkey-search#303](https://github.com/valkey-io/valkey-search/pull/303) ([tests](tests/valkey-search/303-a16926323/)): Allow running single integration test (#303): declined: The change modifies only developer tooling: build.sh gains a --run-integration-tests=pattern option to filter which integration tests run, scripts/common.rc fixes a shell test for an unset SAN_BUILD variable and a log message typo. No source file of the valkey-search module is touched, so 
- [valkey-io/valkey-search#284](https://github.com/valkey-io/valkey-search/pull/284) ([tests](tests/valkey-search/284-fba1a3822/)): Add coordinator metadata health monitoring info field (#284): declined: no proposed test validated
- [valkey-io/valkey-search#285](https://github.com/valkey-io/valkey-search/pull/285) ([tests](tests/valkey-search/285-7bea873c0/)): Adding command flags to match ACL permissions. (#285): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search#298](https://github.com/valkey-io/valkey-search/pull/298) ([tests](tests/valkey-search/298-ca45eeb0a/)): build.sh now runs both integration test suits (#298): declined: The change is purely build/CI/test infrastructure: build.sh now also runs the OSS integration suite (with retries), shell helpers moved into scripts/common.rc (setup_valkey_server, setup_json_module, install_test_framework, check_for_san_errors), .gitignore entries were added, and the Pyth
- [valkey-io/valkey-search#288](https://github.com/valkey-io/valkey-search/pull/288) ([tests](tests/valkey-search/288-e001f91ca/)): Fix num_docs not getting reset across a flushall. Add flushall test (#288): 0 of 2 test(s) reached the change; crash, predates #288, since fixed (recorded)
- [valkey-io/valkey-search#283](https://github.com/valkey-io/valkey-search/pull/283) ([tests](tests/valkey-search/283-8b53ed55a/)): Reject queries when no valid pre-filter and no vector filter is provided (#283): 3 of 3 test(s) reached the change; crash, fixed by #283 (recorded)
- [valkey-io/valkey-search#268](https://github.com/valkey-io/valkey-search/pull/268) ([tests](tests/valkey-search/268-0e8cc8c89/)): Handle OOM during search command fanout (#268): declined: no proposed test validated
- [valkey-io/valkey-search#281](https://github.com/valkey-io/valkey-search/pull/281) ([tests](tests/valkey-search/281-288cfb0a5/)): Change visibility for some of info fields to developer only (#281): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#275](https://github.com/valkey-io/valkey-search/pull/275) ([tests](tests/valkey-search/275-8782ca876/)): Remove unnecessary file added in #249 (#275): declined: The change only removes a test-build CMakeLists.txt (testing/coordinator/CMakeLists.txt, duplicate build rules for unit tests added in #249) and adds a trailing space to a comment in src/indexes/text/posting.h. No runtime source, command handling, indexing, replication or persistence code 
- [valkey-io/valkey-search#209](https://github.com/valkey-io/valkey-search/pull/209) ([tests](tests/valkey-search/209-1be979bbb/)): Add config to skip loading index from rdb (#209): 2 of 2 test(s) reached the change; crash, predates #209, since fixed (recorded)
- [valkey-io/valkey-search#249](https://github.com/valkey-io/valkey-search/pull/249) ([tests](tests/valkey-search/249-40355d3b8/)): Reimplement timeout handling. (#249): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#237](https://github.com/valkey-io/valkey-search/pull/237) ([tests](tests/valkey-search/237-54ffb1b04/)): Add Comprehensive Tests for Time Slice Mutex Metrics (#237): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search#248](https://github.com/valkey-io/valkey-search/pull/248) ([tests](tests/valkey-search/248-526c4c585/)): Fix WrongArity function to wrap command names with single quotes for Valkey consistency (#248): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#246](https://github.com/valkey-io/valkey-search/pull/246) ([tests](tests/valkey-search/246-75b9e3184/)): Fix ACL bug where commands where allowed when user has no permissions for one of the index prefixes (#246): 2 of 2 test(s) reached the change

## Totals

- Merges whose tests reached the change: 533 of 772 tested (69%)
- Changed-line coverage: median 100% of 378 merge(s) measured, 24 under 50%
- Recall: 152 parents flagged of 224 fix merges
- Claude: $1808.45 in the last 24 hours, $5516.60 in the last 7 days, $5516.60 in all
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
