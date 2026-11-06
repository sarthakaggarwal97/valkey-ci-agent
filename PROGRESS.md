# Fuzzer walk

Updated 2026-11-06T05:35+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 269 | 2225 | #4562 (2026-09-09) | 244 | 38.4 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 81 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 28 | 8.9 |
| valkey-io/valkey-search | 170 | 498 | #1122 (2026-06-16) | 170 | 24.3 |

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

[2026-11-06T05:28:41+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42168580154): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-06T05:35:04+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42169210073): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-06T03:49:08+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42136285020): 20 merge(s) tested, 3 without a code change.

- [valkey-io/valkey-search#1207](https://github.com/valkey-io/valkey-search/pull/1207) ([tests](tests/valkey-search/1207-49a585b70/)): Open next development cycle on main (release stage "dev", drop stale CMake VERSION) (#1207): declined: The change only opens the next development cycle: it flips the MODULE_RELEASE_STAGE macro from "rc2" to "dev" and drops the stale VERSION 1.0.0 from the CMake project() call. MODULE_RELEASE_STAGE is defined in src/version.h and referenced nowhere else in the tree (not in INFO output, any c
- [valkey-io/valkey-search#1191](https://github.com/valkey-io/valkey-search/pull/1191) ([tests](tests/valkey-search/1191-7204f2f2c/)): Validate HNSW index data when loading from an external file (#1191): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search#1199](https://github.com/valkey-io/valkey-search/pull/1199) ([tests](tests/valkey-search/1199-226928b62/)): Replace main branch release notes with development placeholder (#1199): declined: The change edits only the 00-RELEASENOTES documentation file, swapping the 1.0.0 release notes for a development-branch placeholder. No module source, command behaviour, configuration, or build logic changes, so no runtime path can differ between the two builds and no scenario is needed.
- [valkey-io/valkey-search#1203](https://github.com/valkey-io/valkey-search/pull/1203) ([tests](tests/valkey-search/1203-5a63e8269/)): Reset search.search-result-background-cleanup back to true (#1203): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1202](https://github.com/valkey-io/valkey-search/pull/1202) ([tests](tests/valkey-search/1202-5c07b8b53/)): Async Index Schema Destruction (#1202): 0 of 2 test(s) reached the change
- [valkey-io/valkey-search#1192](https://github.com/valkey-io/valkey-search/pull/1192) ([tests](tests/valkey-search/1192-ae132bea8/)): Validate query vector size against index dimensions at parse time (#1192): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1163](https://github.com/valkey-io/valkey-search/pull/1163) ([tests](tests/valkey-search/1163-323c7dc1d/)): Vector Search performance improvement (#1163): 0 of 6 test(s) reached the change
- [valkey-io/valkey-search#1082](https://github.com/valkey-io/valkey-search/pull/1082) ([tests](tests/valkey-search/1082-f6f9cafa4/)): Enable thread-safety checking, fix violations it found. (#1082): 0 of 2 test(s) reached the change
- [valkey-io/valkey-search#1174](https://github.com/valkey-io/valkey-search/pull/1174) ([tests](tests/valkey-search/1174-cf1c9bb26/)): JSON string values with backslash escapes are indexed un-decoded (#1174): 0 of 2 test(s) reached the change
- [valkey-io/valkey-search#914](https://github.com/valkey-io/valkey-search/pull/914) ([tests](tests/valkey-search/914-fa920966a/)): Allow FT._debug to use when client is admin (#914): 3 of 3 test(s) reached the change; [crash, introduced by #914](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/72)
- [valkey-io/valkey-search#1015](https://github.com/valkey-io/valkey-search/pull/1015) ([tests](tests/valkey-search/1015-067b45462/)): Replace deque with flat hash map for tracked_vectors (#1015): declined: no proposed test validated
- [valkey-io/valkey-search#1087](https://github.com/valkey-io/valkey-search/pull/1087) ([tests](tests/valkey-search/1087-417428561/)): Drain in-flight async query operations before shutting down in cancel/timeout tests (#1087): declined: no proposed test validated
- [valkey-io/valkey-search#1120](https://github.com/valkey-io/valkey-search/pull/1120) ([tests](tests/valkey-search/1120-5d95e4c75/)): Guard against disengaged optional in InTrackedMutationRecords (#1120): declined: The change adds a has_value() guard to IndexSchema::InTrackedMutationRecords plus a unit test, but that function has no production callers at either the parent or the change commit: grepping the whole valkey-search tree, the only references are its declaration (src/index_schema.h:513), its
- [valkey-io/valkey-search#1032](https://github.com/valkey-io/valkey-search/pull/1032) ([tests](tests/valkey-search/1032-29ab9f304/)): Workaround fix for ostringstream crash (#1032): 0 of 3 test(s) reached the change
- [valkey-io/valkey-search#1046](https://github.com/valkey-io/valkey-search/pull/1046) ([tests](tests/valkey-search/1046-ada7859e8/)): Fix coordinator bytes-in counting before response mutation (#1046): declined: no proposed test validated
- [valkey-io/valkey-search#1031](https://github.com/valkey-io/valkey-search/pull/1031) ([tests](tests/valkey-search/1031-d900f0155/)): Reimplement TAG field using RAX tree to save space. (#1031): 0 of 5 test(s) reached the change
- [valkey-io/valkey-search#999](https://github.com/valkey-io/valkey-search/pull/999) ([tests](tests/valkey-search/999-6c30c7cfd/)): Fix crash when loading values of type text from JSON in FT.AGGREGATE (#999): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1079](https://github.com/valkey-io/valkey-search/pull/1079) ([tests](tests/valkey-search/1079-ce011fb69/)): Fix Race condition in VectorHNSW::AddRecordImpl during concurrent HSET with HNSW vector index (#1079): 0 of 5 test(s) reached the change
- [valkey-io/valkey-search#1060](https://github.com/valkey-io/valkey-search/pull/1060) ([tests](tests/valkey-search/1060-59c44b44b/)): Fix race in UnTrackedKeys ingestion access vs negative queries (#1060): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1148](https://github.com/valkey-io/valkey-search/pull/1148) ([tests](tests/valkey-search/1148-4b2c0859a/)): Fix tag query parsing to support escaped closing brace (#979) + resolved merge conflicts (#1148): 2 of 2 test(s) reached the change

## Totals

- Merges whose tests reached the change: 254 of 338 tested (75%)
- Changed-line coverage: median 100% of 144 merge(s) measured, 7 under 50%
- Recall: 74 parents flagged of 97 fix merges
- Claude: $1989.39 in the last 24 hours, $2196.96 in the last 7 days, $2196.96 in all
- Issues filed: 28
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

## Retries

- [valkey-io/valkey#4425](https://github.com/valkey-io/valkey/pull/4425): attempt 2 of 3, 1 of 30 run(s) could not start: the run could not start: runner execution failed during deployment.provision: LocalDeploymentError: Instance valkey-0 failed to start within 30s (process output from /h
- [valkey-io/valkey#4005](https://github.com/valkey-io/valkey/pull/4005): attempt 2 of 3, 2 of 40 run(s) could not start: the run could not start: runner execution failed during deployment.provision: LocalDeploymentError: Instance valkey-0 failed to start within 30s (process output from /h
- [valkey-io/valkey#4076](https://github.com/valkey-io/valkey/pull/4076): attempt 1 of 3, 1 of 50 run(s) could not start: the run could not start: runner execution failed during deployment.provision: LocalDeploymentError: Instance valkey-0 failed to start within 30s (process output from /h
- [valkey-io/valkey@ed9dc9fa82c6](https://github.com/valkey-io/valkey/commit/ed9dc9fa82c6134bb6c14e73ba20966e92441fcd): attempt 1 of 3, 2 of 7 run(s) could not start: the run could not start: runner execution failed during deployment.provision: LocalDeploymentError: Instance valkey-0 failed to start within 30s (process output from /ho
- [valkey-io/valkey#3438](https://github.com/valkey-io/valkey/pull/3438): attempt 1 of 3, 1 of 65 run(s) could not start: the run could not start: runner execution failed during deployment.provision: LocalDeploymentError: slot assignment did not propagate within 20s: 16384/16384 assigned, 
