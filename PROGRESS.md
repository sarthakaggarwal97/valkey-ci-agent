# Fuzzer walk

Updated 2026-11-06T06:36+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 291 | 2204 | #3708 (2026-09-01) | 247 | 41.6 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 81 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 26 | 8.9 |
| valkey-io/valkey-search | 170 | 498 | #1122 (2026-06-16) | 146 | 24.3 |

## Last runs

### valkey-io/valkey

[2026-11-06T06:27:05+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42154311502): 20 merge(s) tested, 7 without a code change.

- [valkey-io/valkey#4425](https://github.com/valkey-io/valkey/pull/4425): Add cluster-replica-no-failover if-empty to block failover from empty replicas (#4425): error: 2 of 25 run(s) could not start: the run could not start: runner execution failed during deployment.provision: LocalDeploymentError: Instance valkey-0 failed to start within 60s (process output from /home/runner/work/valkey-ci-agent/valkey-ci-agent/walk-work/runs/ai-44e01c0da86d-parent-202/art
- [valkey-io/valkey#4005](https://github.com/valkey-io/valkey/pull/4005): Reserve connection slots for priority sources (#4005): error: 1 of 40 run(s) could not start: the run could not start: runner execution failed during deployment.provision: LocalDeploymentError: Instance valkey-0 failed to start within 60s (process output from /home/runner/work/valkey-ci-agent/valkey-ci-agent/walk-work/runs/ai-641393ea5cb2-parent-40058/a
- [valkey-io/valkey#4076](https://github.com/valkey-io/valkey/pull/4076): to retry, 2 of 50 run(s) could not start: the run could not start: runner execution failed during deployment.provision: LocalDeploymentError: Instance valkey-0 failed to start within 60s (process output from /h
- [valkey-io/valkey@ed9dc9fa82c6](https://github.com/valkey-io/valkey/commit/ed9dc9fa82c6134bb6c14e73ba20966e92441fcd): to retry, 2 of 7 run(s) could not start: the run could not start: runner execution failed during deployment.provision: LocalDeploymentError: Instance valkey-0 failed to start within 60s (process output from /ho
- [valkey-io/valkey#3438](https://github.com/valkey-io/valkey/pull/3438): to retry, 2 of 71 run(s) could not start: the run could not start: runner execution failed during deployment.provision: LocalDeploymentError: slot assignment did not propagate within 20s: 16384/16384 assigned, 
- [valkey-io/valkey#3940](https://github.com/valkey-io/valkey/pull/3940) ([tests](tests/valkey/3940-2d44c7499/)): Nested prefetching for hash and zset inner hashtables (#3940): 0 of 5 test(s) reached the change, coverage 91%
- [valkey-io/valkey#4332](https://github.com/valkey-io/valkey/pull/4332) ([tests](tests/valkey/4332-e1126ce52/)): valkey-benchmark: remove cross-thread contention from the hot path (#4332): 4 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4610](https://github.com/valkey-io/valkey/pull/4610) ([tests](tests/valkey/4610-85d02f638/)): Fix duplicate reply on FLUSHDB/FLUSHALL with invalid args in forkless (#4610): 2 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4609](https://github.com/valkey-io/valkey/pull/4609) ([tests](tests/valkey/4609-da9d58f1b/)): improper int to bit-flag conversion (#4609): 2 of 4 test(s) reached the change, coverage 33%
- [valkey-io/valkey#4330](https://github.com/valkey-io/valkey/pull/4330) ([tests](tests/valkey/4330-bb741667d/)): Reject non-migration clients for CLUSTER SYNCSLOTS FINISH (#4330): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4603](https://github.com/valkey-io/valkey/pull/4603) ([tests](tests/valkey/4603-0a631f046/)): revert typo config change (#4603): declined: The change modifies only .config/typos.toml, the configuration of the typos spell-check CI linter (replacing broad acronym ignore regexes with an explicit allowlist). It touches no server, tool, or test code, so there is no runtime behaviour to test and no scenario is needed.
- [valkey-io/valkey#4460](https://github.com/valkey-io/valkey/pull/4460): to retry, 1 of 34 run(s) could not start: the run could not start: runner execution failed during deployment.provision: LocalDeploymentError: Instance valkey-0 failed to start within 60s (process output from /h
- [valkey-io/valkey#4595](https://github.com/valkey-io/valkey/pull/4595) ([tests](tests/valkey/4595-351eb0c36/)): Tear down repl throttler when the last replica disconnects (#4595): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4587](https://github.com/valkey-io/valkey/pull/4587) ([tests](tests/valkey/4587-075b7d5cb/)): Fix scripts_mem accounting for LRU script SHA copies (#4587): 2 of 2 test(s) reached the change, coverage 91%; [check, introduced by #4587](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/73)
- [valkey-io/valkey#4017](https://github.com/valkey-io/valkey/pull/4017) ([tests](tests/valkey/4017-5c11f8cb9/)): Optimize HMGET, SMISMEMBER and ZMSCORE with batched hashtable lookup (#4017): 0 of 3 test(s) reached the change, coverage 97%
- [valkey-io/valkey#4171](https://github.com/valkey-io/valkey/pull/4171) ([tests](tests/valkey/4171-d19563211/)): Fix ping_sent getting stuck when peer traffic keeps link alive (#4171): 1 of 4 test(s) reached the change, coverage 83%
- [valkey-io/valkey#4586](https://github.com/valkey-io/valkey/pull/4586) ([tests](tests/valkey/4586-12f933131/)): tests/rdma: improve diagnostics for sporadic connection failures (#4586): declined: The change is diagnostics-only: a CI workflow tweak (always run dmesg), reworded serverLog error messages in src/rdma.c that add strerror()/error codes without altering any branch condition, return value, or behaviour, and expanded fatal diagnostics in the standalone RDMA test harness (tes
- [valkey-io/valkey#4356](https://github.com/valkey-io/valkey/pull/4356): to retry, 2 of 37 run(s) could not start: the run could not start: runner execution failed during deployment.provision: LocalDeploymentError: Instance valkey-0 failed to start within 60s (process output from /h
- [valkey-io/valkey#4362](https://github.com/valkey-io/valkey/pull/4362) ([tests](tests/valkey/4362-205fde3d3/)): Fix test_reclaimFilePageCache to avoid tmpfs (#4362): declined: The change only fixes an inverted GTEST_SKIP condition in the C++ unit test TestReclaimFilePageCache (src/unit/test_util.cpp), making the test skip on tmpfs-backed /tmp where fadvise(FADV_DONTNEED) is a no-op. It is a test/CI-only fix with no change to any Valkey server, command, replicati
- [valkey-io/valkey#4566](https://github.com/valkey-io/valkey/pull/4566) ([tests](tests/valkey/4566-7507a93a7/)): Include module version in module load log message (#4566): declined: The change only edits the text of two serverLog NOTICE messages printed when a module loads, adding the module version to the log line. It does not alter any command reply, module behaviour, data structure, replication, or persistence path, so there is no observable behaviour for an experi

### valkey-io/valkey-bloom

[2026-11-06T06:30:09+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42174748550): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-06T06:36:22+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42175436068): 0 merge(s) tested, 0 without a code change.


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

- Merges whose tests reached the change: 260 of 347 tested (75%)
- Changed-line coverage: median 100% of 153 merge(s) measured, 8 under 50%
- Recall: 77 parents flagged of 100 fix merges
- Claude: $1909.91 in the last 24 hours, $2288.77 in the last 7 days, $2288.77 in all
- Issues filed: 29
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

## Retries

- [valkey-io/valkey#4076](https://github.com/valkey-io/valkey/pull/4076): attempt 2 of 3, 2 of 50 run(s) could not start: the run could not start: runner execution failed during deployment.provision: LocalDeploymentError: Instance valkey-0 failed to start within 60s (process output from /h
- [valkey-io/valkey@ed9dc9fa82c6](https://github.com/valkey-io/valkey/commit/ed9dc9fa82c6134bb6c14e73ba20966e92441fcd): attempt 2 of 3, 2 of 7 run(s) could not start: the run could not start: runner execution failed during deployment.provision: LocalDeploymentError: Instance valkey-0 failed to start within 60s (process output from /ho
- [valkey-io/valkey#3438](https://github.com/valkey-io/valkey/pull/3438): attempt 2 of 3, 2 of 71 run(s) could not start: the run could not start: runner execution failed during deployment.provision: LocalDeploymentError: slot assignment did not propagate within 20s: 16384/16384 assigned, 
- [valkey-io/valkey#4460](https://github.com/valkey-io/valkey/pull/4460): attempt 1 of 3, 1 of 34 run(s) could not start: the run could not start: runner execution failed during deployment.provision: LocalDeploymentError: Instance valkey-0 failed to start within 60s (process output from /h
- [valkey-io/valkey#4356](https://github.com/valkey-io/valkey/pull/4356): attempt 1 of 3, 2 of 37 run(s) could not start: the run could not start: runner execution failed during deployment.provision: LocalDeploymentError: Instance valkey-0 failed to start within 60s (process output from /h
