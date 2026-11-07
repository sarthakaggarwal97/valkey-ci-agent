# Fuzzer walk

Updated 2026-11-07T11:25+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 612 | 1895 | #3509 (2026-04-17) | 300 | 87.4 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 393 | 276 | #603 (2026-01-15) | 165 | 56.1 |

## Last runs

### valkey-io/valkey

[2026-11-07T10:39:11+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42354781729): 20 merge(s) tested, 5 without a code change.

- [valkey-io/valkey#3578](https://github.com/valkey-io/valkey/pull/3578): Fix Deferred Reply Placeholders in Active Deferred Buffers (#3578): error: 2 of 2 run(s) could not start: the run could not start: environment acquisition failed
- [valkey-io/valkey#3554](https://github.com/valkey-io/valkey/pull/3554) ([tests](tests/valkey/3554-28ecbd204/)): Ensure client slot migration pointer is cleared during reset (#3554): 0 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3548](https://github.com/valkey-io/valkey/pull/3548) ([tests](tests/valkey/3548-a3e44a55d/)): Fix lua-enable-insecure-api default value cannot be changed to yes (#3548): declined: no proposed test validated
- [valkey-io/valkey#3557](https://github.com/valkey-io/valkey/pull/3557) ([tests](tests/valkey/3557-ac9ca9de3/)): Fix rdmaServer leaks when create listen cm id error (#3557): declined: The change only fixes a resource leak (event channel and addrinfo not freed) on an error path in rdmaServer when rdma_create_id fails during RDMA listener setup. It has no observable behaviour change: the listen attempt fails with the same error either way. Reaching the path requires an RD
- [valkey-io/valkey#3533](https://github.com/valkey-io/valkey/pull/3533) ([tests](tests/valkey/3533-bb8866557/)): hashtable: fix dismissHashtable madvise size (#3533): 0 of 6 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3555](https://github.com/valkey-io/valkey/pull/3555) ([tests](tests/valkey/3555-edc0d26ad/)): Strip LTO flags from static Lua module build (#3555): declined: The change is build-system only: it strips LTO flags (-flto, -flto=auto, -ffat-lto-objects) from the static Lua module Makefile so that archiving into libvalkeylua.a does not produce LTO bitcode objects the system linker cannot read. No runtime behaviour, command reply, replication, persis
- [valkey-io/valkey@a7d495352ac0](https://github.com/valkey-io/valkey/commit/a7d495352ac0ba724eeec51fd57b615e8e805f52) ([tests](tests/valkey/a7d495352/)): extra UT: declined: The change adds only a new unit test (safe_iterator_release_before_cleanup) to src/unit/test_hashtable.cpp; no server, tool, or library code changes. There is no runtime behaviour to exercise, so no scenario is needed.
- [valkey-io/valkey@54980ece3a46](https://github.com/valkey-io/valkey/commit/54980ece3a46a1bf83eea40870542c09c843b8fa) ([tests](tests/valkey/54980ece3/)): hashtable iterator safety: invalidate on exhaustion: 0 of 9 test(s) reached the change, coverage 100%, 4 skipped (required capability 'build' is unsupported)
- [valkey-io/valkey#3545](https://github.com/valkey-io/valkey/pull/3545) ([tests](tests/valkey/3545-d2db0c268/)): Fix module commandresult event cleanup during unsubscribe and module unload (#3545): 0 of 5 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3504](https://github.com/valkey-io/valkey/pull/3504) ([tests](tests/valkey/3504-5abf79e0e/)): Add zmalloc_aligned() and fix SPMC queue buffer alignment (#3504): 0 of 5 test(s) reached the change, coverage 81%
- [valkey-io/valkey#3535](https://github.com/valkey-io/valkey/pull/3535) ([tests](tests/valkey/3535-970984344/)): Optimize HGETDEL to pause auto shrink when deleting multiple items (#3535): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3541](https://github.com/valkey-io/valkey/pull/3541) ([tests](tests/valkey/3541-651c40a89/)): Fix FD leak in connSocketBlockingConnect on timeout (#3541): declined: The change only moves conn->fd = fd before the aeWait timeout check in connSocketBlockingConnect, closing an fd that was previously leaked on a connect timeout. Its sole caller is MIGRATE to an unresponsive target. The client-visible behaviour is identical on both builds: both send -IOERR 
- [valkey-io/valkey#3498](https://github.com/valkey-io/valkey/pull/3498): to retry, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#3458](https://github.com/valkey-io/valkey/pull/3458) ([tests](tests/valkey/3458-3ab9d9797/)): Fix race condition during async client freeing with IO threading enabled (#3458): 0 of 3 test(s) reached the change, coverage 33%
- [valkey-io/valkey#3518](https://github.com/valkey-io/valkey/pull/3518) ([tests](tests/valkey/3518-fc00f7be0/)): Fix VLA warning in io_threads (#3518): declined: The change only replaces a `const int BATCH_SIZE = 32` local with `#define BATCH_SIZE 32` in io_threads.c so the `batch_jobs[BATCH_SIZE]` array is no longer a compiler-flagged VLA. The array size and all runtime behavior are identical; this is a warning-silencing refactor with no observabl
- [valkey-io/valkey#2936](https://github.com/valkey-io/valkey/pull/2936): to retry, 5 of 5 run(s) could not start: the run could not start: environment acquisition failed
- [valkey-io/valkey#3520](https://github.com/valkey-io/valkey/pull/3520) ([tests](tests/valkey/3520-9d51f5ff8/)): Document VALKEYCLI_HOST/PORT variables in help (#3520): declined: The change only edits the static usage/help text printed by valkey-cli (documenting the pre-existing VALKEYCLI_HOST/VALKEYCLI_PORT environment variables for -h and -p). No code path that parses options, reads the environment variables, connects, or executes commands is modified, so there i
- [valkey-io/valkey#3392](https://github.com/valkey-io/valkey/pull/3392) ([tests](tests/valkey/3392-0327c2713/)): Add Static Module Support (#3392): 6 of 15 test(s) reached the change, coverage 59%
- [valkey-io/valkey#3306](https://github.com/valkey-io/valkey/pull/3306) ([tests](tests/valkey/3306-269b1c5ed/)): Improve COB memory tracking with copy avoidance (#3306): 0 of 4 test(s) reached the change, coverage 95%
- [valkey-io/valkey#3516](https://github.com/valkey-io/valkey/pull/3516) ([tests](tests/valkey/3516-4a42c9585/)): Fix HPERSIST RESP protocol violation on wrong-type key (#3516): 2 of 2 test(s) reached the change, coverage 100%

### valkey-io/valkey-bloom

[2026-11-07T11:24:36+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42368019056): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-07T11:25:52+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42368108950): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-07T07:50:34+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42342331075): 20 merge(s) tested, 1 without a code change.

- [valkey-io/valkey-search#741](https://github.com/valkey-io/valkey-search/pull/741) ([tests](tests/valkey-search/741-b0e8a26b5/)): Minor fix for build failures (#741): declined: The change only adds a local declaration `auto &schema_manager = SchemaManager::Instance();` so the pre-existing `schema_manager.GetIndexSchema(...)` call in QueryCommand::Execute compiles — a pure build fix for a compile error introduced by the parent commit. It binds the same singleton t
- [valkey-io/valkey-search#735](https://github.com/valkey-io/valkey-search/pull/735) ([tests](tests/valkey-search/735-960d136f7/)): Solving Text Negation by using a Universal Set + Filtering (#735): 6 of 6 test(s) reached the change; crash, predates #735, since fixed (recorded)
- [valkey-io/valkey-search#734](https://github.com/valkey-io/valkey-search/pull/734) ([tests](tests/valkey-search/734-c5e1e78f1/)): Remove redundant has_text_predicate field (#734): declined: Pure refactor with no reachable behaviour change. The removed has_text_predicate boolean was set only at the end of FilterParser::ParseTextTokens, the exact function whose every successful exit already sets QueryOperations::kContainsText (filter_parser.cc:874,882), so the new QueryHasTextP
- [valkey-io/valkey-search#621](https://github.com/valkey-io/valkey-search/pull/621) ([tests](tests/valkey-search/621-9ea236a2a/)): Minor code quality improvements (#621): declined: The change is a pure refactor with no behavior change in the module: it removes an unused local variable and renames a lambda parameter in QueryCommand::Execute (src/commands/commands.cc), and makes kDefaultMinStemSize constexpr in ft_create_parser.h — the constant is only ever read (ft_cr
- [valkey-io/valkey-search#607](https://github.com/valkey-io/valkey-search/pull/607) ([tests](tests/valkey-search/607-e41ee6069/)): Endurance test (#607): declined: The change "Endurance test (#607)" modifies only Python test-infrastructure files (testing/integration/stability_runner.py, stability_test.py, utils.py), adding new index-type variants and text attributes to valkey-search's own stability/endurance test harness. No module source code is cha
- [valkey-io/valkey-search#638](https://github.com/valkey-io/valkey-search/pull/638) ([tests](tests/valkey-search/638-f90b05072/)): Optimize non nested pure text composed and queries by skipping pre filtering (#638): 0 of 3 test(s) reached the change; crash, introduced and since fixed (recorded)
- [valkey-io/valkey-search#633](https://github.com/valkey-io/valkey-search/pull/633) ([tests](tests/valkey-search/633-357043189/)): Allow local node to have no primary endpoint (#633): declined: no proposed test validated
- [valkey-io/valkey-search#521](https://github.com/valkey-io/valkey-search/pull/521) ([tests](tests/valkey-search/521-97a513224/)): Use new command "FT.Internal_update" to replicate index create/drop (#521): 2 of 5 test(s) reached the change; [crash, introduced by #521](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/87); crash, introduced and since fixed (recorded)
- [valkey-io/valkey-search#634](https://github.com/valkey-io/valkey-search/pull/634) ([tests](tests/valkey-search/634-85ba2606c/)): Update deduplication check to check Tag existence (#634): 3 of 3 test(s) reached the change; crash, predates #634, since fixed (recorded)
- [valkey-io/valkey-search#631](https://github.com/valkey-io/valkey-search/pull/631) ([tests](tests/valkey-search/631-2d9a3b539/)): Fix ordering of neighbors in CME (#631): declined: The fix changes behaviour only inside SearchPartitionResultsTracker in src/query/fanout.cc, which is constructed solely by PerformSearchFanoutAsync, and that fanout runs only when UsingCoordinator() && IsCluster() (src/commands/commands.cc:139-167). UsingCoordinator() requires the hidden '
- [valkey-io/valkey-search#629](https://github.com/valkey-io/valkey-search/pull/629) ([tests](tests/valkey-search/629-016e983be/)): Avoid deadlock while processing  MULTI/EXEC mutations (#629): declined: no proposed test validated
- [valkey-io/valkey-search#626](https://github.com/valkey-io/valkey-search/pull/626) ([tests](tests/valkey-search/626-130e9274e/)): Add CMAKE_POLICY_VERSION_MINIMUM to build scripts and submodules (#626): declined: The change only adds -DCMAKE_POLICY_VERSION_MINIMUM=3.5 to build.sh and to the submodules' common CMake arguments so that newer CMake (4.x) accepts the submodules' old cmake_minimum_required declarations at configure time. It is a build/CI configuration change with no modification to any s
- [valkey-io/valkey-search#558](https://github.com/valkey-io/valkey-search/pull/558) ([tests](tests/valkey-search/558-5431493cb/)): Update PARAMS argument for ft.aggregate documentation (#558): declined: The change edits only src/commands/ft.aggregate.json, a command-documentation metadata file describing FT.AGGREGATE's PARAMS argument. That file is not compiled into the module (src/commands/CMakeLists.txt lists only .cc sources), is referenced by no source, build, or load path, and the mo
- [valkey-io/valkey-search#624](https://github.com/valkey-io/valkey-search/pull/624) ([tests](tests/valkey-search/624-5a1f3ab61/)): Optimize all proximity queries and pure OR queries (#624): 0 of 2 test(s) reached the change
- [valkey-io/valkey-search#610](https://github.com/valkey-io/valkey-search/pull/610) ([tests](tests/valkey-search/610-ffd4bf08d/)): Refactoring SendReply block (#610): declined: This is a pure behavior-preserving refactor of the reply-generation code in ft_aggregate.cc (SendReplyInner split into ProcessNeighborsForProcessing, ProcessFieldValue, CreateRecordsFromNeighbors, ExecuteAggregationStages, GenerateResponse) and ft_search.cc (SendReply split into HandleEarl
- [valkey-io/valkey-search#620](https://github.com/valkey-io/valkey-search/pull/620) ([tests](tests/valkey-search/620-143f2c61e/)): Add SeekForwardPosition API to TextIterator interface and all the derived iterators (#620): declined: no proposed test validated
- [valkey-io/valkey-search#616](https://github.com/valkey-io/valkey-search/pull/616) ([tests](tests/valkey-search/616-3f11e51a3/)): Implement backslash escape character handling (#616): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#617](https://github.com/valkey-io/valkey-search/pull/617) ([tests](tests/valkey-search/617-c2c24916b/)): Implement aggregation_metrics (#617): 2 of 2 test(s) reached the change; crash, predates #617, since fixed (recorded)
- [valkey-io/valkey-search#613](https://github.com/valkey-io/valkey-search/pull/613) ([tests](tests/valkey-search/613-10d16a1b8/)): String Intern race fix, This is a recreation of PR #576. (#613): 0 of 3 test(s) reached the change; crash, predates #613, since fixed (recorded)
- [valkey-io/valkey-search#611](https://github.com/valkey-io/valkey-search/pull/611) ([tests](tests/valkey-search/611-5a230b259/)): Eliminate redundant mainthread filter invokations (#611): declined: no proposed test validated

## Totals

- Merges whose tests reached the change: 416 of 605 tested (69%)
- Changed-line coverage: median 100% of 286 merge(s) measured, 18 under 50%
- Recall: 124 parents flagged of 182 fix merges
- Claude: $1677.00 in the last 24 hours, $4290.50 in the last 7 days, $4290.50 in all
- Issues filed: 43
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

## Retries

- [valkey-io/valkey#3800](https://github.com/valkey-io/valkey/pull/3800): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#3613](https://github.com/valkey-io/valkey/pull/3613): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#3498](https://github.com/valkey-io/valkey/pull/3498): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#2936](https://github.com/valkey-io/valkey/pull/2936): attempt 1 of 3, 5 of 5 run(s) could not start: the run could not start: environment acquisition failed
