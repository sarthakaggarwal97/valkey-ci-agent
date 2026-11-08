# Fuzzer walk

Updated 2026-11-08T00:37+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 812 | 1695 | #3076 (2026-01-20) | 356 | 116.0 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 530 | 139 | #218 (2025-07-21) | 196 | 75.7 |

## Last runs

### valkey-io/valkey

[2026-11-07T23:06:41+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42415598670): 20 merge(s) tested, 7 without a code change.

- [valkey-io/valkey#3126](https://github.com/valkey-io/valkey/pull/3126) ([tests](tests/valkey/3126-1cc268de9/)): Fix mixup of reply-larger-than and request-larger-than (#3126): 4 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3067](https://github.com/valkey-io/valkey/pull/3067) ([tests](tests/valkey/3067-8a9c3717d/)): Don't exit the process when locale-collate fails to be set from env during startup (#3067): declined: The only behaviour change is at startup: when locale-collate is at its empty default and setlocale(LC_COLLATE, "") fails, the server now continues with the default locale instead of exit(1). That branch is taken only when the valkey-server process's own environment variables (LC_ALL/LC_COL
- [valkey-io/valkey#2867](https://github.com/valkey-io/valkey/pull/2867) ([tests](tests/valkey/2867-920bf128b/)): Optimize skiplist query efficiency by embedding the skiplist header (#2867): 0 of 7 test(s) reached the change, coverage 93%
- [valkey-io/valkey#2928](https://github.com/valkey-io/valkey/pull/2928) ([tests](tests/valkey/2928-1fd6d7156/)): decouple lru/lfu from server.h (#2928): 2 of 5 test(s) reached the change, coverage 86%, 1 skipped (required capability 'build' is unsupported: default: build rejects startup configuration: *** FATAL CONFIG FILE ERROR (Version 255.255.255) *** Reading the configuration file, at line 11 >>> 'forkless)
- [valkey-io/valkey#2953](https://github.com/valkey-io/valkey/pull/2953) ([tests](tests/valkey/2953-f5d7dc8d0/)): Fix potential memory leaks when a writable replica is promoted to primary after direct writes of keys with expiration (#2953): 1 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2999](https://github.com/valkey-io/valkey/pull/2999) ([tests](tests/valkey/2999-eb3f465e5/)): Fail fast on invalid certificates at TLS config load (#2999): declined: The change only adds certificate-validity checks in src/tls.c's createSSLContext, reached when a TLS-enabled build loads a TLS config (at startup or on CONFIG SET of a tls-* option). Exercising the new branch needs a BUILD_TLS=yes binary, an active TLS listener, and deliberately expired/no
- [valkey-io/valkey#3103](https://github.com/valkey-io/valkey/pull/3103) ([tests](tests/valkey/3103-0422fc66c/)): Enable HW clock by default (#3103): 1 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3117](https://github.com/valkey-io/valkey/pull/3117) ([tests](tests/valkey/3117-928e91261/)): remove unneeded include of expire.h (#3117): declined: The change only removes an unneeded #include "expire.h" from src/t_hash.c. No executable code, reply, data format, or configuration changes; any breakage would be a compile failure caught by the build, not a runtime behaviour a fuzzer scenario could reach.
- [valkey-io/valkey#2463](https://github.com/valkey-io/valkey/pull/2463) ([tests](tests/valkey/2463-4ad7628d0/)): Instrumenting used active time metrics for I/O threads (#2463): 2 of 4 test(s) reached the change, coverage 93%
- [valkey-io/valkey#3086](https://github.com/valkey-io/valkey/pull/3086) ([tests](tests/valkey/3086-985cf836a/)): Perf: Track net bytes only if commandlog-request-larger-than != -1 (#3086): 3 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3060](https://github.com/valkey-io/valkey/pull/3060) ([tests](tests/valkey/3060-94edae4b8/)): Fix how hash is handling overriding of expired fields overwrite (#3060): 4 of 11 test(s) reached the change, coverage 83%
- [valkey-io/valkey#3107](https://github.com/valkey-io/valkey/pull/3107) ([tests](tests/valkey/3107-e7d004be0/)): Comment cleanup around entry.c file (#3107): declined: The change is comment and whitespace cleanup in entry.c. The only two code edits are behavior-neutral: (1) entryGetExpiryRef's debugServerAssert was upgraded to serverAssert, but all three callers (entryGetExpiry, entrySetExpiry, and the guarded call in entryUpdateAsStringRef) already esta
- [valkey-io/valkey#3015](https://github.com/valkey-io/valkey/pull/3015) ([tests](tests/valkey/3015-278607b74/)): makefile formatting: split lists onto multiple lines for better readability and easier merging (#3015): declined: The change is purely a Makefile formatting refactor: the ENGINE_SERVER_OBJ, ENGINE_TRACE_OBJ, ENGINE_CLI_OBJ, ENGINE_BENCHMARK_OBJ and Lua-module object lists are split onto multiple lines and sorted alphabetically, with the identical set of object files before and after. No source file, c
- [valkey-io/valkey#3073](https://github.com/valkey-io/valkey/pull/3073) ([tests](tests/valkey/3073-d8315cfcf/)): Improve performance during rehashing (#3073): 0 of 6 test(s) reached the change, coverage 100%, 1 skipped (required capability 'build' is unsupported: default: build rejects startup configuration: *** FATAL CONFIG FILE ERROR (Version 255.255.255) *** Reading the configuration file, at line 11 >>> 'forkless)
- [valkey-io/valkey#3098](https://github.com/valkey-io/valkey/pull/3098) ([tests](tests/valkey/3098-be0a572c8/)): Fix minor inconsistency in connFormatAddr size argument (#3098): declined: The change replaces sizeof(addr) with sizeof(laddr) in a connFormatAddr call, but both buffers are declared with the same size (char [CONN_ADDR_STR_LEN]), so the passed value is the identical compile-time constant. It is a pure code-consistency refactor with no behaviour change: no reply, 
- [valkey-io/valkey#2742](https://github.com/valkey-io/valkey/pull/2742) ([tests](tests/valkey/2742-231a9e731/)): Fix XREAD returning error on empty stream with + ID (#2742): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3088](https://github.com/valkey-io/valkey/pull/3088) ([tests](tests/valkey/3088-2e2eb418f/)): Fix the memory leak in the VM_GetCommandKeysWithFlags function (#3088): 0 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3022](https://github.com/valkey-io/valkey/pull/3022) ([tests](tests/valkey/3022-4b5a48a36/)): Fix HRANDFIELD to return null response when no field could be found (#3022): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3020](https://github.com/valkey-io/valkey/pull/3020) ([tests](tests/valkey/3020-3c092caa0/)): Automatic TLS reload (#3020): 2 of 7 test(s) reached the change, coverage 0%
- [valkey-io/valkey#3081](https://github.com/valkey-io/valkey/pull/3081) ([tests](tests/valkey/3081-1084e671c/)): improve format for hashtableDump (#3081): declined: The change only reworks printf formatting inside hashtableDump, a debug dump helper in src/hashtable.c. The function is not called from any server code path — its only reference besides the definition is a forward declaration in the unit test file src/unit/test_hashtable.c — so no command 

### valkey-io/valkey-bloom

[2026-11-08T00:26:26+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42429708252): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-08T00:09:29+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42428414440): 0 merge(s) tested, 0 without a code change.


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

- Merges whose tests reached the change: 521 of 756 tested (69%)
- Changed-line coverage: median 100% of 363 merge(s) measured, 23 under 50%
- Recall: 148 parents flagged of 220 fix merges
- Claude: $1917.83 in the last 24 hours, $5401.59 in the last 7 days, $5401.59 in all
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
