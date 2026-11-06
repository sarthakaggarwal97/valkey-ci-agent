# Fuzzer walk

Updated 2026-11-06T03:25+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 269 | 2225 | #4562 (2026-09-09) | 263 | 38.4 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 105 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 56 | 8.9 |
| valkey-io/valkey-search | 147 | 521 | #1207 (2026-07-01) | 147 | 21.0 |

## Last runs

### valkey-io/valkey

[2026-11-06T03:04:52+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42137571247): 20 merge(s) tested, 2 without a code change.

- [valkey-io/valkey#5298](https://github.com/valkey-io/valkey/pull/5298) ([tests](tests/valkey/5298-85b827c39/)): Size the bgiteration queue on items left over from the last feed (#5298): 0 of 5 test(s) reached the change, coverage 83%
- [valkey-io/valkey#4425](https://github.com/valkey-io/valkey/pull/4425): to retry, 1 of 30 run(s) could not start: the run could not start: runner execution failed during deployment.provision: LocalDeploymentError: Instance valkey-0 failed to start within 30s (process output from /h
- [valkey-io/valkey#4005](https://github.com/valkey-io/valkey/pull/4005): to retry, 2 of 40 run(s) could not start: the run could not start: runner execution failed during deployment.provision: LocalDeploymentError: Instance valkey-0 failed to start within 30s (process output from /h
- [valkey-io/valkey#4076](https://github.com/valkey-io/valkey/pull/4076): to retry, 1 of 50 run(s) could not start: the run could not start: runner execution failed during deployment.provision: LocalDeploymentError: Instance valkey-0 failed to start within 30s (process output from /h
- [valkey-io/valkey#4257](https://github.com/valkey-io/valkey/pull/4257) ([tests](tests/valkey/4257-089321adf/)): perf: combined post-command pending-work gate in afterCommand() (#4257): 0 of 6 test(s) reached the change, coverage 100%; crash, fixed by #4257 (recorded)
- [valkey-io/valkey#4670](https://github.com/valkey-io/valkey/pull/4670) ([tests](tests/valkey/4670-18277411d/)): Reject repeated CLUSTER SYNCSLOTS ESTABLISH on the same connection (#4670): 2 of 2 test(s) reached the change, coverage 100%; crash, fixed by #4670 (recorded)
- [valkey-io/valkey#4669](https://github.com/valkey-io/valkey/pull/4669) ([tests](tests/valkey/4669-1e90169ef/)): Track field TTLs in the volatile set when a HASH_2 load converts mid-listpack (#4669): 1 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4617](https://github.com/valkey-io/valkey/pull/4617) ([tests](tests/valkey/4617-5810f5be9/)): Remove duplicate c->flag.blocked check (#4617): declined: The change removes a redundant `!c->flag.blocked` check inside a block that is only entered when `!c->flag.blocked` already holds (the enclosing `if (update_command_stats && !c->flag.blocked)` in call() in src/server.c). The inner condition's truth value is unchanged in every reachable sta
- [valkey-io/valkey#4624](https://github.com/valkey-io/valkey/pull/4624) ([tests](tests/valkey/4624-da91ccd12/)): Fix double-promotion compilation error in test_util.cpp (#4624): declined: The change touches only the unit test file src/unit/test_util.cpp, changing a NaN-producing literal from double (0.0/0.0) to long double (0.0L/0.0L) to fix a double-promotion compilation error in the test build. No server, command, replication, persistence, or tool code is modified, so the
- [valkey-io/valkey#3717](https://github.com/valkey-io/valkey/pull/3717) ([tests](tests/valkey/3717-03a7447bd/)): Add support for secondary certificates (#3717): 3 of 3 test(s) reached the change, coverage 71%
- [valkey-io/valkey#3522](https://github.com/valkey-io/valkey/pull/3522) ([tests](tests/valkey/3522-13c66b82e/)): Fail fast on empty CA directory at TLS config load (#3522): declined: The change makes loadCaCertDir (src/tls.c) fail TLS context creation when tls-ca-cert-dir opens but contains no loadable CA certs. Reaching it requires a BUILD_TLS build plus a currently-valid server cert/key and CA material loaded before the changed function runs, and active TLS (tls-port
- [valkey-io/valkey@4be324e8878f](https://github.com/valkey-io/valkey/commit/4be324e8878fb0332353ad4886fa3940dbd23f91) ([tests](tests/valkey/4be324e88/)): Streaming Compression support for fullsync: 0 of 18 test(s) reached the change, coverage 76%, 5 skipped (required capability 'build' is unsupported)
- [valkey-io/valkey@ed9dc9fa82c6](https://github.com/valkey-io/valkey/commit/ed9dc9fa82c6134bb6c14e73ba20966e92441fcd): to retry, 2 of 7 run(s) could not start: the run could not start: runner execution failed during deployment.provision: LocalDeploymentError: Instance valkey-0 failed to start within 30s (process output from /ho
- [valkey-io/valkey#4653](https://github.com/valkey-io/valkey/pull/4653) ([tests](tests/valkey/4653-98f915212/)): Fix XPENDING reply schema for empty pending lists (#4653): declined: The change only edits the reply_schema metadata in src/commands/xpending.json to document the existing [0, null, null, null] summary reply that XPENDING already returns for an empty pending list. No C code changes: the server's runtime reply for XPENDING is identical on both builds, and re
- [valkey-io/valkey#3538](https://github.com/valkey-io/valkey/pull/3538) ([tests](tests/valkey/3538-95e0e9ac7/)): Add AUTH options to CLUSTER MIGRATESLOTS (#3538): 4 of 4 test(s) reached the change, coverage 96%; check, introduced and since fixed (recorded); check, not reproduced (recorded); check, introduced and since fixed (recorded); check, not reproduced (recorded); check, introduced and since fixed (recorded)
- [valkey-io/valkey#4629](https://github.com/valkey-io/valkey/pull/4629) ([tests](tests/valkey/4629-d46565268/)): Implement XACKDEL and XDELEX command (#4629): 5 of 7 test(s) reached the change, coverage 98%
- [valkey-io/valkey#3212](https://github.com/valkey-io/valkey/pull/3212) ([tests](tests/valkey/3212-c80da010b/)): Listpack encoding for Hash field expiration (#3212): 6 of 16 test(s) reached the change, coverage 77%
- [valkey-io/valkey#4643](https://github.com/valkey-io/valkey/pull/4643) ([tests](tests/valkey/4643-202a48058/)): bgiteration: fix unit-test feed deadlock under slow execution (#4643): declined: The change is test-infrastructure only. The single C hunk makes bgIteration_feedIterators_task ignore its time budget when called with eventLoop == NULL, and the only NULL-eventLoop callers are the unit-test helper bgIteration_feedIterators() and the main-thread branch of bgIteratorRead, w
- [valkey-io/valkey#4487](https://github.com/valkey-io/valkey/pull/4487) ([tests](tests/valkey/4487-f9f1b3ee3/)): Making the module global-defrag callback usable (#4487): 0 of 4 test(s) reached the change, coverage 85%
- [valkey-io/valkey#3438](https://github.com/valkey-io/valkey/pull/3438): to retry, 1 of 65 run(s) could not start: the run could not start: runner execution failed during deployment.provision: LocalDeploymentError: slot assignment did not propagate within 20s: 16384/16384 assigned, 

### valkey-io/valkey-bloom

[2026-11-06T03:25:00+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42154551628): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-06T03:01:28+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42153882874): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-06T00:21:55+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42113110732): 20 merge(s) tested, 2 without a code change.

- [valkey-io/valkey-search#946](https://github.com/valkey-io/valkey-search/pull/946) ([tests](tests/valkey-search/946-a5233dfd9/)): feat: add First Value reducer to FT.Aggregate groupby stage (#946): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search#1171](https://github.com/valkey-io/valkey-search/pull/1171) ([tests](tests/valkey-search/1171-1a2306b10/)): Fix indexing for zero-length keys (#1171): 3 of 3 test(s) reached the change; crash, introduced and since fixed (recorded)
- [valkey-io/valkey-search#1281](https://github.com/valkey-io/valkey-search/pull/1281) ([tests](tests/valkey-search/1281-8fc3bbdc0/)): Lower per-field info definition log from WARNING to DEBUG (#1281): declined: The change only lowers the log level of a per-field 'Defined Info Field' diagnostic message from WARNING to DEBUG in vmsdk/src/info.cc. It alters no command behavior, replies, data, replication, or configuration — just server log verbosity, which the fuzzer's oracles do not observe. There 
- [valkey-io/valkey-search#1265](https://github.com/valkey-io/valkey-search/pull/1265) ([tests](tests/valkey-search/1265-bb1f1c723/)): Fix replica reporting incorrect search_number_of_indexes after full sync (#1265): declined: no proposed test validated
- [valkey-io/valkey-search#1230](https://github.com/valkey-io/valkey-search/pull/1230) ([tests](tests/valkey-search/1230-92efaa600/)): Fix SIGSEGV in LatencySampler when hdr_init fails (#1230): declined: The fix only changes behaviour when hdr_init fails inside LatencySampler::SubmitSample. In the shipped module every LatencySampler is constructed with hard-coded valid parameters (min=1ns, max=1s, precision=2 in src/metrics.h), so hdr_init can only fail on calloc returning NULL inside the 
- [valkey-io/valkey-search#1283](https://github.com/valkey-io/valkey-search/pull/1283) ([tests](tests/valkey-search/1283-459878064/)): Fix duplicate label handling (#1283): declined: The change fixes HNSW duplicate-label handling in two paths, both unreachable by valkey-fuzzer. (1) The modify-path fix (vector_hnsw.cc ModifyRecordImpl dropping markDelete, and hnswalg.h addPoint reusing an existing label's slot) only changes behavior when allow_replace_deleted_ is true. 
- [valkey-io/valkey-search#1172](https://github.com/valkey-io/valkey-search/pull/1172) ([tests](tests/valkey-search/1172-7a7b94538/)): Fix up test_skip_index_load.py (#1172): declined: The change modifies only a Python integration test (test_skip_index_load.py, refactored to use the test framework's create_server instead of raw subprocess) and a CI build script (scripts/common.rc, adding an ELF linker flag so ASAN sanitizer builds load libstdc++ early). No module source,
- [valkey-io/valkey-search#1276](https://github.com/valkey-io/valkey-search/pull/1276) ([tests](tests/valkey-search/1276-53ae957fd/)): feat: overload protection - 2 mechanisms to prevent an ever growing query queue  (#1276): 4 of 4 test(s) reached the change
- [valkey-io/valkey-search#932](https://github.com/valkey-io/valkey-search/pull/932) ([tests](tests/valkey-search/932-a1f2159c6/)): feat: add TOLIST reducer for FT.AGGREGATE GROUPBY stage (#932): 4 of 4 test(s) reached the change
- [valkey-io/valkey-search#1259](https://github.com/valkey-io/valkey-search/pull/1259) ([tests](tests/valkey-search/1259-578a75a37/)): fix hardcoded db_num in metadata_manager.cc (#1259): declined: The fix is inside coordinator::MetadataManager::OnLoadingEnded, which only exists when valkey-search is started in cluster mode with the --use-coordinator module argument (valkey_search.cc:1185 creates the MetadataManager only under GetUseCoordinator() && IsCluster()). That option can only
- [valkey-io/valkey-search#832](https://github.com/valkey-io/valkey-search/pull/832) ([tests](tests/valkey-search/832-320aec93e/)): Replace blocks with token field in json files (#832): declined: The change only rewrites the command-spec JSON metadata files (ft.aggregate.json, ft.create.json, ft.search.json) from nested pure-token blocks to the compact token-field notation. These files are documentation/metadata artifacts: they are not listed in any CMakeLists, not embedded in the 
- [valkey-io/valkey-search#1030](https://github.com/valkey-io/valkey-search/pull/1030) ([tests](tests/valkey-search/1030-e028db6c0/)): Reimplement Numeric field for 3x space reduction (#1030): 0 of 4 test(s) reached the change
- [valkey-io/valkey-search#1155](https://github.com/valkey-io/valkey-search/pull/1155) ([tests](tests/valkey-search/1155-3e3825c01/)): Revise handling of keys with fields with invalid values to match Redisearch (#1155): 1 of 5 test(s) reached the change
- [valkey-io/valkey-search#1086](https://github.com/valkey-io/valkey-search/pull/1086) ([tests](tests/valkey-search/1086-6f297274c/)): Fix crashes in expression evaluation. Add compatibility fixes. (#1086): 6 of 6 test(s) reached the change; crash, fixed by #1086 (recorded)
- [valkey-io/valkey-search#893](https://github.com/valkey-io/valkey-search/pull/893) ([tests](tests/valkey-search/893-79e1b3e7d/)): feat: Add array value type support to expression system (#893): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1231](https://github.com/valkey-io/valkey-search/pull/1231) ([tests](tests/valkey-search/1231-af1ca3ee8/)): Revise outdated text index comments (#1231): declined: The change only revises comments in rax_wrapper.h and text_index.h and deletes an unused private member (track_subtree_item_counts_) and its unused accessor (TrackSubtreeItemsCountEnabled) — a grep over the repository at the change shows no remaining references, so it was dead code. No run
- [valkey-io/valkey-search#1226](https://github.com/valkey-io/valkey-search/pull/1226) ([tests](tests/valkey-search/1226-a9a8b9ad2/)): Handle kOneReplicaPerShard in GetTargetsForSlot for single-slot indexes (#1226): declined: no proposed test validated
- [valkey-io/valkey-search#1222](https://github.com/valkey-io/valkey-search/pull/1222) ([tests](tests/valkey-search/1222-87978b530/)): The query vector is not normalized on the prefiltering query execution path (#1222): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1223](https://github.com/valkey-io/valkey-search/pull/1223) ([tests](tests/valkey-search/1223-9e51a16ec/)): Fixing debug mode compilation failure related to simsimd (#1223): declined: The change only adds `static` to three `inline` SIMD helper functions in vendored simsimd headers to fix a debug-mode (-O0) compilation/linkage failure. It changes symbol linkage, not any function body: distance and log2 computations, and thus all module commands and replies, are identical
- [valkey-io/valkey-search#1187](https://github.com/valkey-io/valkey-search/pull/1187) ([tests](tests/valkey-search/1187-e8e94d700/)): RemoveSubscription improper destruction handling fix (#1187): declined: This is a replica-only crash fix on the diskless-RDB-load error path, and the valkey-fuzzer has no way to reach or demonstrate it. (1) The bug requires a diskless replica full sync (src/index_schema.cc:1614 LoadFromRDB -> LoadIndexExtension), which only happens over replication with at lea

## Totals

- Merges whose tests reached the change: 247 of 324 tested (76%)
- Changed-line coverage: median 100% of 144 merge(s) measured, 7 under 50%
- Recall: 71 parents flagged of 91 fix merges
- Claude: $2000.79 in the last 24 hours, $2051.24 in the last 7 days, $2051.24 in all
- Issues filed: 27
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

## Retries

- [valkey-io/valkey#4425](https://github.com/valkey-io/valkey/pull/4425): attempt 2 of 3, 1 of 30 run(s) could not start: the run could not start: runner execution failed during deployment.provision: LocalDeploymentError: Instance valkey-0 failed to start within 30s (process output from /h
- [valkey-io/valkey#4005](https://github.com/valkey-io/valkey/pull/4005): attempt 2 of 3, 2 of 40 run(s) could not start: the run could not start: runner execution failed during deployment.provision: LocalDeploymentError: Instance valkey-0 failed to start within 30s (process output from /h
- [valkey-io/valkey#4076](https://github.com/valkey-io/valkey/pull/4076): attempt 1 of 3, 1 of 50 run(s) could not start: the run could not start: runner execution failed during deployment.provision: LocalDeploymentError: Instance valkey-0 failed to start within 30s (process output from /h
- [valkey-io/valkey@ed9dc9fa82c6](https://github.com/valkey-io/valkey/commit/ed9dc9fa82c6134bb6c14e73ba20966e92441fcd): attempt 1 of 3, 2 of 7 run(s) could not start: the run could not start: runner execution failed during deployment.provision: LocalDeploymentError: Instance valkey-0 failed to start within 30s (process output from /ho
- [valkey-io/valkey#3438](https://github.com/valkey-io/valkey/pull/3438): attempt 1 of 3, 1 of 65 run(s) could not start: the run could not start: runner execution failed during deployment.provision: LocalDeploymentError: slot assignment did not propagate within 20s: 16384/16384 assigned, 
