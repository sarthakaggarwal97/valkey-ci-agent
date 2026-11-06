# Fuzzer walk

Updated 2026-11-06T15:34+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 358 | 2140 | #3971 (2026-08-11) | 224 | 51.1 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 1 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 255 | 413 | #903 (2026-03-13) | 178 | 36.4 |

## Last runs

### valkey-io/valkey

[2026-11-06T13:39:02+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42220168667): 20 merge(s) tested, 0 without a code change.

- [valkey-io/valkey#4401](https://github.com/valkey-io/valkey/pull/4401) ([tests](tests/valkey/4401-1e7a8f310/)): Skip IO-thread read-done followup unless update_state sync-invokes handlers (#4401): 0 of 3 test(s) reached the change, coverage 50%
- [valkey-io/valkey#4360](https://github.com/valkey-io/valkey/pull/4360) ([tests](tests/valkey/4360-afabf680a/)): Fix use-after-free crash when cluster messages arrive after module unload (#4360): declined: The fix removes a module's cluster-message receivers when the module is unloaded (moduleUnregisterCleanup -> moduleUnregisterClusterReceivers), preventing a use-after-free when a cluster-bus message of that type arrives afterward and moduleCallClusterReceivers dispatches to the freed modul
- [valkey-io/valkey#4402](https://github.com/valkey-io/valkey/pull/4402) ([tests](tests/valkey/4402-abebdb020/)): Correct the `since` version on CONFIG INFO and MOVE REPLACE to 9.2.0 (#4402): 2 of 2 test(s) reached the change
- [valkey-io/valkey#2247](https://github.com/valkey-io/valkey/pull/2247) ([tests](tests/valkey/2247-3f16ffa04/)): Consolidating nonexistent spelling (#2247): declined: The change is a spelling consolidation (#2247): every hunk in the C sources (acl.c, bitops.c, hyperloglog.c, module.c, networking.c, rax.c, t_set.c, t_stream.c) edits only comment text, replacing 'non existing'/'non-existent' with 'nonexistent'. The remaining hunks rename test titles and p
- [valkey-io/valkey#4381](https://github.com/valkey-io/valkey/pull/4381) ([tests](tests/valkey/4381-8cec1092b/)): Validate stream listpack live and deleted record counts on load (#4381): 2 of 2 test(s) reached the change, coverage 83%; [check, introduced by #4381](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/79); [crash, introduced by #4381](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/80)
- [valkey-io/valkey#2239](https://github.com/valkey-io/valkey/pull/2239) ([tests](tests/valkey/2239-7b374fcf7/)): Grammar corrections in comments (#2239): declined: The change (PR #2239) only corrects grammar in comments across C sources, utils scripts and Tcl test helpers, plus rewords one static SENTINEL HELP usage string. No code path, reply semantics, config, persistence, replication or protocol behaviour is altered, so there is nothing a scenario
- [valkey-io/valkey#2253](https://github.com/valkey-io/valkey/pull/2253) ([tests](tests/valkey/2253-bb3b927f2/)): Improve spelling and grammar (#2253): declined: The change is purely cosmetic: every hunk edits a code comment, a Tcl test name/comment, or valkey.conf commentary. No executable statement, error message, reply string, log string, or config default changes — the geo.c edit fixes a usage comment, the module.c/rdb.c/replication.c/rdma.c/sc
- [valkey-io/valkey#4368](https://github.com/valkey-io/valkey/pull/4368) ([tests](tests/valkey/4368-d9ce598b0/)): Remove duplicate unreachable "command" branch in fuzzer arg generator (#4368): declined: The change deletes a duplicate, unreachable else-if branch in src/fuzzer_command_generator.c, Valkey's in-tree fuzzing harness code. It is dead-code removal in test tooling, not in the server: no command behavior, reply, replication, persistence, or cluster path changes, so no scenario can
- [valkey-io/valkey#2249](https://github.com/valkey-io/valkey/pull/2249) ([tests](tests/valkey/2249-2c38467d3/)): Fixes grammatical and spelling errors (#2249): declined: The diff only fixes spelling and grammar: code comments, README and valkey.conf comment text, test-suite message strings, and a consistent rename of the local variable restore_replicy to restore_policy in functionRestoreCommand (src/functions.c), with every read and write of the variable r
- [valkey-io/valkey#3729](https://github.com/valkey-io/valkey/pull/3729) ([tests](tests/valkey/3729-6cdff5ef0/)): Fix grammar errors in code comments (#3729): declined: The change (#3729) only fixes grammar in C code comments across aof.c, cluster_legacy.c, db.c, entry.c, eval.c, latency.c, module.c, t_set.c, t_stream.c, util.c, valkey-check-aof.c, valkeymodule.h and ziplist.c. No executable statement, constant, reply text, or configuration is touched, so
- [valkey-io/valkey#3922](https://github.com/valkey-io/valkey/pull/3922) ([tests](tests/valkey/3922-967c7cc8e/)): Harden stream validation on RDB load against crafted metadata (#3922): 2 of 2 test(s) reached the change, coverage 91%; [check, introduced by #3922](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/79); [crash, introduced by #3922](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/80)
- [valkey-io/valkey#2243](https://github.com/valkey-io/valkey/pull/2243) ([tests](tests/valkey/2243-975cfc102/)): Correct spelling in module cleanup, command docs, source comments, and Tcl tests (#2243): declined: The change is purely spelling fixes: comments and docs in bitops.c, defrag.c, lolwut5.c, module.c, replication.c, rio.c, server.c, siphash.c, src/commands/README.md and valkey.conf, plus test-name/comment typos in Tcl tests. The only code edit renames the internal function moduleRemoveCate
- [valkey-io/valkey#4323](https://github.com/valkey-io/valkey/pull/4323) ([tests](tests/valkey/4323-875696ffe/)): Clear the redaction bitmap between commands in a transaction (#4323): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4380](https://github.com/valkey-io/valkey/pull/4380) ([tests](tests/valkey/4380-db7da3d11/)): Resolve a key's slot from the key, not from the executing command (#4380): 2 of 9 test(s) reached the change, coverage 100%
- [valkey-io/valkey@cc45d0637a00](https://github.com/valkey-io/valkey/commit/cc45d0637a00d59d41cd1e1315de1ced19e78982) ([tests](tests/valkey/cc45d0637/)): Fix various typos around otherwise with a comma: declined: Every hunk in the diff is a comment or documentation punctuation fix (adding a comma after 'Otherwise') in C source comments, valkey.conf comments, Tcl test comments, a Ruby utility comment, and one line of valkey-cli's --latency help text. No executable code, reply, protocol, persistence,
- [valkey-io/valkey@42aff62b3094](https://github.com/valkey-io/valkey/commit/42aff62b3094489d3b6726f9907f92285bb1d3a5) ([tests](tests/valkey/42aff62b3/)): Fix "setup" related typos: declined: The change only fixes 'setup' vs 'set up' typos in C comments, module example comments, and TCL test descriptions. No executable code, command behaviour, reply, config, or protocol path is altered, so there is nothing a scenario could exercise or compare.
- [valkey-io/valkey#4388](https://github.com/valkey-io/valkey/pull/4388) ([tests](tests/valkey/4388-5813754ff/)): Fix include order in custom_matchers.hpp (#4388): declined: The change only adds an #include to src/unit/custom_matchers.hpp, a header used exclusively by the unit-test build (gtest/gmock matchers). It is a build/test-infrastructure fix with no change to server, replication, persistence, command, or tool behaviour, so no scenario can exercise it an
- [valkey-io/valkey#2244](https://github.com/valkey-io/valkey/pull/2244) ([tests](tests/valkey/2244-94d221cdd/)): Spelling 7 (#2244): declined: The change is a spelling-fix commit that edits only code comments in C sources and comment/message strings in Tcl test files. No executable code path, command reply, config, or protocol behavior is altered, so there is no behavior change to test and no scenario is needed.
- [valkey-io/valkey#3972](https://github.com/valkey-io/valkey/pull/3972) ([tests](tests/valkey/3972-119aac6ac/)): Validate PUBLISH and MODULE payload lengths against packet size (#3972): declined: This change only hardens the cluster-bus packet validator (clusterIsValidPacket in src/cluster_legacy.c) against integer-overflow from attacker-controlled length fields (channel_len/message_len/module payload len) in FORGED PUBLISH/PUBLISHSHARD/MODULE packets. The new reject branches fire 
- [valkey-io/valkey#4327](https://github.com/valkey-io/valkey/pull/4327) ([tests](tests/valkey/4327-4e441522e/)): Skip static Lua engine loading in Sentinel mode (#4327): declined: The change only skips static Lua engine loading when the process starts in Sentinel mode. Non-sentinel servers (the only kind the fuzzer can deploy: standalone and cluster) take the exact same code path as the parent build, so no compared command, tool run, or fault scenario can observe a 

### valkey-io/valkey-bloom

[2026-11-06T15:34:13+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42244179411): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-06T15:03:59+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42239327231): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-06T12:55:05+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42201766408): 20 merge(s) tested, 7 without a code change.

- [valkey-io/valkey-search#939](https://github.com/valkey-io/valkey-search/pull/939) ([tests](tests/valkey-search/939-dcc1214f1/)): Shorten grpc timeout (#939): 1 of 1 test(s) reached the change
- [valkey-io/valkey-search#942](https://github.com/valkey-io/valkey-search/pull/942) ([tests](tests/valkey-search/942-1e5c7459a/)): Skip building unit test files for integration test workflows (#942): declined: The change only touches build tooling: CMakeLists.txt renames the BUILD_TESTS option to BUILD_UNIT_TESTS, build.sh passes the renamed flag and allows CMAKE_EXTRA_ARGS from the environment, and ci/build_ubuntu.sh disables unit-test compilation when a CI run executes integration tests only. 
- [valkey-io/valkey-search#941](https://github.com/valkey-io/valkey-search/pull/941) ([tests](tests/valkey-search/941-6362bc1b0/)): Fix RDB load memory leak (#941): declined: no proposed test validated
- [valkey-io/valkey-search#934](https://github.com/valkey-io/valkey-search/pull/934) ([tests](tests/valkey-search/934-0b51c5595/)): Add memory reservation based on mutation queue (#934): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#926](https://github.com/valkey-io/valkey-search/pull/926) ([tests](tests/valkey-search/926-ef1f6193a/)): Allow replacement of deleted hnsw elements (#926): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search#931](https://github.com/valkey-io/valkey-search/pull/931) ([tests](tests/valkey-search/931-03f66fa81/)): Improve test output (#931): declined: The change modifies only build.sh (build/test-runner script: ICU build output redirected to a log file with failure tails, and per-test output files so only the failed test's output is dumped) and removes std::cout debug prints from unit tests in testing/coordinator/metadata_manager_test.c
- [valkey-io/valkey-search#930](https://github.com/valkey-io/valkey-search/pull/930) ([tests](tests/valkey-search/930-d1bf81158/)): Adding yield during mutation queue drainage for save and restore (#930): 0 of 3 test(s) reached the change
- [valkey-io/valkey-search#921](https://github.com/valkey-io/valkey-search/pull/921) ([tests](tests/valkey-search/921-de0bb244d/)): Slot Ownership check (#921): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#860](https://github.com/valkey-io/valkey-search/pull/860) ([tests](tests/valkey-search/860-e972c8f71/)): Some tsan fixes (#860): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#827](https://github.com/valkey-io/valkey-search/pull/827) ([tests](tests/valkey-search/827-eff3d337c/)): adding the commented out tests (#827): declined: The change only edits valkey-search's own Python integration test suite (testing/integration/stability_test.py): it re-enables three previously commented-out stability test cases (text/tag/numeric with backfill, coordinator and replica) and turns on background ftcreate/ftdropindex/flushdb 
- [valkey-io/valkey-search#871](https://github.com/valkey-io/valkey-search/pull/871) ([tests](tests/valkey-search/871-4fb4b5fee/)): Reducing mutation queue growth during restore (#871): 3 of 3 test(s) reached the change; crash, predates #871, since fixed (recorded)
- [valkey-io/valkey-search#913](https://github.com/valkey-io/valkey-search/pull/913) ([tests](tests/valkey-search/913-4293fddfd/)): Mark ft info metrics as Dev configs (#913): declined: no proposed test validated
- [valkey-io/valkey-search#912](https://github.com/valkey-io/valkey-search/pull/912) ([tests](tests/valkey-search/912-a2d6da892/)): Fix RDB Load String Memory Leak (#912): 1 of 1 test(s) reached the change; crash, introduced and since fixed (recorded); crash, fixed by #912 (recorded)
- [valkey-io/valkey-search#911](https://github.com/valkey-io/valkey-search/pull/911) ([tests](tests/valkey-search/911-e7b9d6359/)): Change search.hide-user-data-from-log config to Dev and remove total_indexing_time (unimplemented metric) (#911): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#883](https://github.com/valkey-io/valkey-search/pull/883) ([tests](tests/valkey-search/883-fe5945889/)): Implement clean up index schemas on SHUTDOWN event (#883): 0 of 3 test(s) reached the change; crash, predates #883, since fixed (recorded)
- [valkey-io/valkey-search#905](https://github.com/valkey-io/valkey-search/pull/905) ([tests](tests/valkey-search/905-c4fb75f92/)): [DOCUMENTATION] Application Metrics Cleanup (#905): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#909](https://github.com/valkey-io/valkey-search/pull/909) ([tests](tests/valkey-search/909-b9325e833/)): Add history entries for text indexing and search support in v1.2.0 (#909): declined: The change only adds "history" entries ("1.2.0", "Added Text Indexing and Search support") to four command-documentation JSON files (src/commands/ft.aggregate.json, ft.create.json, ft.info.json, ft.search.json). These files are not consumed by the build: src/commands/CMakeLists.txt compile
- [valkey-io/valkey-search#873](https://github.com/valkey-io/valkey-search/pull/873) ([tests](tests/valkey-search/873-7e9d0d6ea/)): Search module - Audit log messages (#873): declined: The change only edits log messages in the valkey-search module: log levels are moved between NOTICE and DEBUG, several logs are rate-limited with VMSDK_LOG_EVERY_N_SEC, error-message details and DEBUG/FANOUT_DEBUG prefixes are removed from log lines for audit purposes, and two test files d
- [valkey-io/valkey-search#906](https://github.com/valkey-io/valkey-search/pull/906) ([tests](tests/valkey-search/906-ea3bfdcbb/)): Add validation to reject attribute aliases containing {}[]:;$ (#906): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#896](https://github.com/valkey-io/valkey-search/pull/896) ([tests](tests/valkey-search/896-1045bb0e0/)): Implement configuration for TAG wildcard minimum prefix length (#896): 2 of 2 test(s) reached the change

## Totals

- Merges whose tests reached the change: 306 of 418 tested (73%)
- Changed-line coverage: median 100% of 183 merge(s) measured, 8 under 50%
- Recall: 89 parents flagged of 124 fix merges
- Claude: $1619.89 in the last 24 hours, $2819.31 in the last 7 days, $2819.31 in all
- Issues filed: 34
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

## Retries

None.
