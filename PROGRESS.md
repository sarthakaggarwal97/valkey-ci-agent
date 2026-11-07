# Fuzzer walk

Updated 2026-11-07T09:53+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 589 | 1918 | #3554 (2026-04-27) | 277 | 84.1 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 393 | 276 | #603 (2026-01-15) | 197 | 56.1 |

## Last runs

### valkey-io/valkey

[2026-11-07T08:16:30+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42347171554): 20 merge(s) tested, 4 without a code change.

- [valkey-io/valkey#3578](https://github.com/valkey-io/valkey/pull/3578): to retry, 3 of 3 run(s) could not start: the run could not start: environment acquisition failed
- [valkey-io/valkey#3619](https://github.com/valkey-io/valkey/pull/3619) ([tests](tests/valkey/3619-fea0b4064/)): Fix invalid memory access in RESTORE with malformed zipmap (CVE-2026-25243) (#3619): 0 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3625](https://github.com/valkey-io/valkey/pull/3625) ([tests](tests/valkey/3625-c7c92db43/)): Delay full sync during yielding Lua scripts to prevent use-after-free (CVE-2026-23631) (#3625): declined: no proposed test validated
- [valkey-io/valkey#3610](https://github.com/valkey-io/valkey/pull/3610) ([tests](tests/valkey/3610-797c62604/)): Fix SIGSEGV in VM_GetLRU/SetLRU/GetLFU/SetLFU on NULL key (#3610): 4 of 4 test(s) reached the change, coverage 100%; crash, fixed by #3610 (recorded)
- [valkey-io/valkey#3583](https://github.com/valkey-io/valkey/pull/3583) ([tests](tests/valkey/3583-8891441ab/)): Fix checkPrefixCollisionsOrReply returning non-zero on self-overlap (#3583): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3330](https://github.com/valkey-io/valkey/pull/3330) ([tests](tests/valkey/3330-cea9354b5/)): Big Endian: add daily workflow UT job and fix UTs (#3330): declined: The change is purely CI and unit-test work: it adds an s390x (big-endian) daily GitHub Actions job, adds/fixes gtest unit tests for endianness round-trips and raw byte layouts, and adds header prototypes (encodeTimeoutKey, decodeTimeoutKey, streamEncodeID) for functions whose definitions i
- [valkey-io/valkey#3601](https://github.com/valkey-io/valkey/pull/3601) ([tests](tests/valkey/3601-46d37e4d5/)): Fix off-by-one boundary in lpEncodeBacklen() for 3 values (#3601): 2 of 2 test(s) reached the change, coverage 100%; [check, predates #3601](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/90); check, predates #3601, since fixed (recorded); crash, predates #3601, since fixed (recorded)
- [valkey-io/valkey#3591](https://github.com/valkey-io/valkey/pull/3591) ([tests](tests/valkey/3591-54bdf5737/)): Handle NULL pointer in streamTrim listpack delta calculation (#3591): 0 of 5 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3596](https://github.com/valkey-io/valkey/pull/3596) ([tests](tests/valkey/3596-cba05103d/)): Fix: prevent NULL dereference crash in connectSlotExportJob when target node disappears (#3596): 0 of 2 test(s) reached the change, coverage 25%
- [valkey-io/valkey#3592](https://github.com/valkey-io/valkey/pull/3592) ([tests](tests/valkey/3592-72fc5b14b/)): Fix compilation error: replace deprecated je_calloc with zcalloc_num (#3592): declined: The change only swaps the deprecated je_calloc for zcalloc_num in allocatorDefragInit to fix a compilation error. zcalloc_num has the same semantics (zeroed memory, calloc signature, non-NULL or abort), so there is no runtime behaviour change to exercise: it is a build fix, and the fact th
- [valkey-io/valkey#3598](https://github.com/valkey-io/valkey/pull/3598) ([tests](tests/valkey/3598-81639e397/)): fix: validate key count before allocating result in keyspec (#3598): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3586](https://github.com/valkey-io/valkey/pull/3586) ([tests](tests/valkey/3586-7e2a2f7c4/)): fix(cluster): Remove per-call srand in clusterManagerNodePrimaryRandom (#3586): 0 of 1 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3580](https://github.com/valkey-io/valkey/pull/3580) ([tests](tests/valkey/3580-678a06d21/)): Set errno on EOF in syncRead and propagate it in logs (#3580): 0 of 7 test(s) reached the change, coverage 66%
- [valkey-io/valkey#3568](https://github.com/valkey-io/valkey/pull/3568) ([tests](tests/valkey/3568-7817ca8a7/)): Fix GEOSEARCH BYPOLYGON leak on invalid COUNT (#3568): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3585](https://github.com/valkey-io/valkey/pull/3585) ([tests](tests/valkey/3585-ad404cd26/)): fix compile warning in util.c (#3585): declined: The change is a build-level fix for a compile warning: it replaces the raw __attribute__((no_sanitize_address, no_sanitize("thread"))) annotations on the string2ll ifunc resolver in src/util.c with the equivalent VALKEY_NO_SANITIZE macros. The resolver body and the string2ll implementation
- [valkey-io/valkey#2823](https://github.com/valkey-io/valkey/pull/2823) ([tests](tests/valkey/2823-39036c7c0/)): Add structured datasets loading capability in valkey benchmark (#2823): 5 of 5 test(s) reached the change, coverage 81%
- [valkey-io/valkey#3584](https://github.com/valkey-io/valkey/pull/3584) ([tests](tests/valkey/3584-16ed690fe/)): fix LTO compilation warning in eval (#3584): declined: Build-only fix: the eval shebang parser is refactored into an extra local and a serverAssert purely to silence an LTO compilation warning. The pre-existing guard at line 255 (sdslen(parts[0]) < 2 returns C_ERR) already guarantees part0_len >= 2 before the assert, so the assert can never tr
- [valkey-io/valkey#3573](https://github.com/valkey-io/valkey/pull/3573) ([tests](tests/valkey/3573-8091c6c10/)): Remove redundant count division in genericHgetallCommand (#3573): declined: The change replaces `setDeferredMapLen(c, replylen, count /= 2); count /= 2;` with `setDeferredMapLen(c, replylen, count / 2);` in genericHgetallCommand. The old compound assignment already passed count/2 to setDeferredMapLen, and the second division only mutated a local variable that is n
- [valkey-io/valkey#3382](https://github.com/valkey-io/valkey/pull/3382) ([tests](tests/valkey/3382-ff80b2d1d/)): Migrate the remaining cluster tests to the new framework and remove legacy files (#2297) (#3382): declined: The change migrates legacy Tcl cluster tests (failover loop, resharding, replica migration, cluster shards) to the new test framework and deletes the old tests/cluster/ harness, runtest-cluster, and updates the daily CI workflow accordingly. No file under src/ is modified, so no server, to
- [valkey-io/valkey#3503](https://github.com/valkey-io/valkey/pull/3503) ([tests](tests/valkey/3503-6dbb7f81a/)): Fix remove cached eval scripts on engine unregister (#3503): declined: The fix only runs inside scriptingEngineManagerUnregister(), reachable solely via VM_UnregisterScriptingEngine() from a scripting-engine module's OnUnload — i.e. MODULE UNLOAD of an engine module (e.g. Lua built as a module). The fuzzer cannot reach it: valkey.action.commands refuses MODUL

### valkey-io/valkey-bloom

[2026-11-07T09:50:04+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42361345045): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-07T09:53:44+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42361592548): 0 merge(s) tested, 0 without a code change.


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

- Merges whose tests reached the change: 414 of 595 tested (70%)
- Changed-line coverage: median 100% of 276 merge(s) measured, 17 under 50%
- Recall: 123 parents flagged of 177 fix merges
- Claude: $1667.05 in the last 24 hours, $4183.49 in the last 7 days, $4183.49 in all
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
- [valkey-io/valkey#3578](https://github.com/valkey-io/valkey/pull/3578): attempt 2 of 3, 3 of 3 run(s) could not start: the run could not start: environment acquisition failed
