# Fuzzer walk

Updated 2026-11-06T02:06+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 252 | 2240 | #4076 (2026-09-14) | 250 | 36.0 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 105 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 56 | 8.9 |
| valkey-io/valkey-search | 147 | 521 | #1207 (2026-07-01) | 147 | 21.0 |

## Last runs

### valkey-io/valkey

[2026-11-06T00:32:24+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42124125580): 20 merge(s) tested, 1 without a code change.

- [valkey-io/valkey#4506](https://github.com/valkey-io/valkey/pull/4506) ([tests](tests/valkey/4506-a84b2f5b9/)): Feature valkey path hash (#4506): 6 of 9 test(s) reached the change, coverage 85%
- [valkey-io/valkey#4232](https://github.com/valkey-io/valkey/pull/4232) ([tests](tests/valkey/4232-23c73ab6f/)): Bump minimum cmake version to 3.24 (#4232): declined: The change is a one-line build-system edit: it raises cmake_minimum_required from 3.10 to 3.24 in CMakeLists.txt. It alters no source code, no command behaviour, no replication/persistence path, and no replies; any build that compiles is byte-for-byte governed by the same sources as before
- [valkey-io/valkey#4664](https://github.com/valkey-io/valkey/pull/4664) ([tests](tests/valkey/4664-3f9062ed5/)): Report each server certificate against the config that loaded it (#4664): declined: The change is confined to src/tls.c and only alters how INFO tls attributes each server certificate's serial/expiry (tls_server_cert_serial, tls_server_alt_cert_serial and their expire-time fields) when dual certificates of different key algorithms are loaded, plus carrying that attributio
- [valkey-io/valkey#4663](https://github.com/valkey-io/valkey/pull/4663) ([tests](tests/valkey/4663-d265ca949/)): Fixes to dual certs (#4663): declined: The only server-code change (src/tls.c) adds the alternate dual-cert key file to the TLS auto-reload change-detection metadata (tlsMaterialsMetadata, captureMetadata, metadataChanged), so a periodic background reload now notices when only the alt key file changed on disk. This path (metada
- [valkey-io/valkey#4676](https://github.com/valkey-io/valkey/pull/4676) ([tests](tests/valkey/4676-d6415e766/)): Fix return types of three module API declarations to match definitions (#4676): declined: The change only corrects three function-pointer declarations in valkeymodule.h from void to int so they match the definitions in module.c, which already returned int (module.c:10480, 10751, 10765), and adds a doc comment. No executable server code changes: modules resolve these APIs by nam
- [valkey-io/valkey#3853](https://github.com/valkey-io/valkey/pull/3853) ([tests](tests/valkey/3853-66f9618f4/)): Streaming Compression support for Replication (#3853): 0 of 11 test(s) reached the change, coverage 87%
- [valkey-io/valkey#4403](https://github.com/valkey-io/valkey/pull/4403) ([tests](tests/valkey/4403-6225ee7c3/)): Add VM_ScanKeyRawBorrowed to allow scanning keys without allocating (#4403): 2 of 2 test(s) reached the change, coverage 93%
- [valkey-io/valkey#866](https://github.com/valkey-io/valkey/pull/866) ([tests](tests/valkey/866-22b3faaa6/)): New maxmemory-scripts config to limit EVAL scripts memory, add DENYOOM flag to SCRIPT LOAD (#866): 4 of 4 test(s) reached the change, coverage 94%
- [valkey-io/valkey#3967](https://github.com/valkey-io/valkey/pull/3967) ([tests](tests/valkey/3967-616399126/)): Add ACL role support (#3967): 7 of 7 test(s) reached the change, coverage 82%
- [valkey-io/valkey#3459](https://github.com/valkey-io/valkey/pull/3459) ([tests](tests/valkey/3459-c0739a2b4/)): Fix missing signalModifiedKey calls for stream commands (#3459): 2 of 2 test(s) reached the change, coverage 88%
- [valkey-io/valkey#2972](https://github.com/valkey-io/valkey/pull/2972) ([tests](tests/valkey/2972-ae2ab6bba/)): Add optional XX to SISMEMBER; return -1 if the key does not exist (#2972): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4665](https://github.com/valkey-io/valkey/pull/4665) ([tests](tests/valkey/4665-74793bd02/)): Check the SORT destination that is actually written (#4665): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4425](https://github.com/valkey-io/valkey/pull/4425): to retry, 1 of 20 run(s) could not start: the run could not start: runner execution failed during deployment.provision: LocalDeploymentError: Instance valkey-0 failed to start within 30s (process output from /h
- [valkey-io/valkey#3253](https://github.com/valkey-io/valkey/pull/3253) ([tests](tests/valkey/3253-6ae7eb488/)): Introduce INCREX command (#3253): 1 of 6 test(s) reached the change, coverage 80%
- [valkey-io/valkey#4554](https://github.com/valkey-io/valkey/pull/4554) ([tests](tests/valkey/4554-cd79e02c1/)): zset bugfix - Fix btree score range resolution and prefix staleness (#4554): 2 of 2 test(s) reached the change, coverage 82%
- [valkey-io/valkey#4497](https://github.com/valkey-io/valkey/pull/4497) ([tests](tests/valkey/4497-754d9f15b/)): zset bugfix - Fix lex range sentinel handling in the ordered index (#4497): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4019](https://github.com/valkey-io/valkey/pull/4019) ([tests](tests/valkey/4019-0adf657af/)): Add `IFEQ`, `IFNE`, `XX`, and `NX` options to `EXEC` command (#4019): 4 of 4 test(s) reached the change, coverage 95%
- [valkey-io/valkey#4005](https://github.com/valkey-io/valkey/pull/4005): to retry, 1 of 30 run(s) could not start: the run could not start: runner execution failed during deployment.provision: LocalDeploymentError: Instance valkey-0 failed to start within 30s (process output from /h
- [valkey-io/valkey#4612](https://github.com/valkey-io/valkey/pull/4612) ([tests](tests/valkey/4612-f26b1609e/)): Fix repl throttle read-handler and throttle_checked ordering, improve tcl tests (#4612): 0 of 6 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4128](https://github.com/valkey-io/valkey/pull/4128) ([tests](tests/valkey/4128-63f56bd16/)): Implement `VM_IncrExternalMemory` and `VM_DecrExternalMemory`  (#4128): 2 of 2 test(s) reached the change, coverage 80%

### valkey-io/valkey-bloom

[2026-11-06T02:06:21+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42140740794): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-06T02:00:00+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42140408802): 0 merge(s) tested, 0 without a code change.


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

- Merges whose tests reached the change: 241 of 314 tested (77%)
- Changed-line coverage: median 100% of 134 merge(s) measured, 7 under 50%
- Recall: 71 parents flagged of 91 fix merges
- Claude: $1909.26 in the last 24 hours, $1945.96 in the last 7 days, $1945.96 in all
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

- [valkey-io/valkey#4425](https://github.com/valkey-io/valkey/pull/4425): attempt 1 of 3, 1 of 20 run(s) could not start: the run could not start: runner execution failed during deployment.provision: LocalDeploymentError: Instance valkey-0 failed to start within 30s (process output from /h
- [valkey-io/valkey#4005](https://github.com/valkey-io/valkey/pull/4005): attempt 1 of 3, 1 of 30 run(s) could not start: the run could not start: runner execution failed during deployment.provision: LocalDeploymentError: Instance valkey-0 failed to start within 30s (process output from /h
