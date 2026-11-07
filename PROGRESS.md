# Fuzzer walk

Updated 2026-11-07T08:03+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 566 | 1941 | #3619 (2026-05-05) | 275 | 80.9 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 393 | 276 | #603 (2026-01-15) | 197 | 56.1 |

## Last runs

### valkey-io/valkey

[2026-11-07T06:28:34+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42340727393): 20 merge(s) tested, 6 without a code change.

- [valkey-io/valkey#5311](https://github.com/valkey-io/valkey/pull/5311) ([tests](tests/valkey/5311-b8d08dfe2/)): Fix uninitialized variable use in clusterManagerMigrateKeysInSlot (#5311): declined: The change fixes an uninitialized-variable read in valkey-cli's clusterManagerMigrateKeysInSlot (src/valkey-cli.c): in the parent build, 'dots', 'reply' and 'migrate_reply' were declared after the cliSelect() call, and the 'if (cliSelect(...) == VALKEY_ERR) goto next;' branch jumps over th
- [valkey-io/valkey#4470](https://github.com/valkey-io/valkey/pull/4470) ([tests](tests/valkey/4470-aa7545348/)): Validate slot import ranges in valkey-check-rdb when checking slot RDB (#4470): 1 of 1 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3374](https://github.com/valkey-io/valkey/pull/3374): Fix module loading failure for redis module (#3374): error: 2 of 2 run(s) could not start: the run could not start: environment acquisition failed
- [valkey-io/valkey#3683](https://github.com/valkey-io/valkey/pull/3683) ([tests](tests/valkey/3683-ad3b33e3b/)): cmake: Fix missing release_header dependency for cli, benchmark, and gtest (#3683): declined: The change only adds CMake add_dependencies(... release_header) lines for valkey-cli, valkey-benchmark, and the gtest library, fixing a parallel-build race where the generated release.h header might not exist yet. It is purely a build-system fix with no change to any binary's runtime behav
- [valkey-io/valkey#3719](https://github.com/valkey-io/valkey/pull/3719) ([tests](tests/valkey/3719-0321a69e6/)): Fix deferred freeClient clobbering replication state after replicaof (#3719): 1 of 8 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3697](https://github.com/valkey-io/valkey/pull/3697) ([tests](tests/valkey/3697-fdf13ca0d/)): Update deps/libvalkey to version 0.5.0 (#3697): 0 of 4 test(s) reached the change
- [valkey-io/valkey#3689](https://github.com/valkey-io/valkey/pull/3689) ([tests](tests/valkey/3689-5b167f455/)): Add check to disallow `CLUSTER SETSLOT` to target self (#3689): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3667](https://github.com/valkey-io/valkey/pull/3667) ([tests](tests/valkey/3667-4344e5b6f/)): Minor cleanup and fix typo in bzmpop get_keys_function (#3667): declined: The change only renames BZMPOP's get_keys_function from blmpopGetKeys to bzmpopGetKeys. Both functions are identical wrappers calling genericGetKeys(0, 2, 3, 1, argv, argc, result) in src/db.c, and BZMPOP/BLMPOP share the same argument layout, so key extraction (COMMAND GETKEYS, cluster sl
- [valkey-io/valkey#3679](https://github.com/valkey-io/valkey/pull/3679) ([tests](tests/valkey/3679-d4337d666/)): Fix CLUSTERSCAN fingerprint to use configurable_hash_seed (#3679): 3 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3544](https://github.com/valkey-io/valkey/pull/3544) ([tests](tests/valkey/3544-fdd903930/)): IO-Threads redesign cleanup work (#3544): 0 of 7 test(s) reached the change, coverage 82%
- [valkey-io/valkey#2993](https://github.com/valkey-io/valkey/pull/2993) ([tests](tests/valkey/2993-ca9dee353/)): Add optional REPLACE argument to MOVE (#2993): 4 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3606](https://github.com/valkey-io/valkey/pull/3606) ([tests](tests/valkey/3606-89b7baa59/)): Speed up cluster startup by 10 seconds (#3606): 1 of 3 test(s) reached the change, coverage 100%; [check, introduced by #3606](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/89)
- [valkey-io/valkey#3613](https://github.com/valkey-io/valkey/pull/3613): to retry, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#3643](https://github.com/valkey-io/valkey/pull/3643) ([tests](tests/valkey/3643-96a6bc5d2/)): Fix the memory leak in valkey-benchmark (#3643): 0 of 1 test(s) reached the change, coverage 50%
- [valkey-io/valkey#3391](https://github.com/valkey-io/valkey/pull/3391) ([tests](tests/valkey/3391-6c9d7fc26/)): CLUSTERSCAN range bounded scanning across contiguous slots (#3391): 4 of 6 test(s) reached the change, coverage 92%
- [valkey-io/valkey#3448](https://github.com/valkey-io/valkey/pull/3448) ([tests](tests/valkey/3448-75fee11c6/)): Fixes server crash when RDMA benchmark clients disconnect (#3448): declined: The change is confined to src/rdma.c: it fixes pending_list corruption (double-add and premature node removal) when an RDMA client disconnects. Exercising it requires a BUILD_RDMA build, RDMA hardware or the rdma_rxe soft-RoCE kernel module (root-only, unavailable in these runs), and a cli
- [valkey-io/valkey#3641](https://github.com/valkey-io/valkey/pull/3641) ([tests](tests/valkey/3641-2f1610771/)): Add null check in updateSSLPendingFlag (#3641): declined: The change adds a defensive null check in updateSSLPendingFlag (src/tls.c). It is unreachable as a behaviour change: the function's only three callers (connTLSRead, connTLSSyncRead, connTLSSyncReadLine) each invoke it immediately after SSL_read(conn->ssl, ...), so on any path where conn->s
- [valkey-io/valkey#3481](https://github.com/valkey-io/valkey/pull/3481) ([tests](tests/valkey/3481-9669b2670/)): Reduce latency spikes during rehashing via incremental page release (#3481): 0 of 5 test(s) reached the change, coverage 93%, 1 skipped (required capability 'build' is unsupported)
- [valkey-io/valkey#3578](https://github.com/valkey-io/valkey/pull/3578): to retry, 3 of 3 run(s) could not start: the run could not start: environment acquisition failed
- [valkey-io/valkey#3492](https://github.com/valkey-io/valkey/pull/3492) ([tests](tests/valkey/3492-c5959266f/)): valkey-benchmark: centralize RDMA WRITABLE kick via createFileEvent (#3492): 0 of 4 test(s) reached the change, coverage 87%

### valkey-io/valkey-bloom

[2026-11-07T08:03:58+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42353813619): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-07T07:34:26+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42351796419): 0 merge(s) tested, 0 without a code change.


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

- Merges whose tests reached the change: 410 of 584 tested (70%)
- Changed-line coverage: median 100% of 265 merge(s) measured, 16 under 50%
- Recall: 120 parents flagged of 169 fix merges
- Claude: $1730.56 in the last 24 hours, $4113.56 in the last 7 days, $4113.56 in all
- Issues filed: 42
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

## Retries

- [valkey-io/valkey#3800](https://github.com/valkey-io/valkey/pull/3800): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#3613](https://github.com/valkey-io/valkey/pull/3613): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#3578](https://github.com/valkey-io/valkey/pull/3578): attempt 1 of 3, 3 of 3 run(s) could not start: the run could not start: environment acquisition failed
