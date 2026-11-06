# Fuzzer walk

Updated 2026-11-06T08:44+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 291 | 2204 | #3708 (2026-09-01) | 227 | 41.6 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 81 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 14 | 8.9 |
| valkey-io/valkey-search | 196 | 472 | #1059 (2026-05-22) | 172 | 28.0 |

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

[2026-11-06T08:13:47+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42186103860): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-06T08:44:10+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42189735925): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-06T06:39:26+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42158810440): 20 merge(s) tested, 6 without a code change.

- [valkey-io/valkey-search#1122](https://github.com/valkey-io/valkey-search/pull/1122) ([tests](tests/valkey-search/1122-72ea56f8c/)): Fix FLAT BLOCK_SIZE 0 livelock and bound INITIAL_CAP/BLOCK_SIZE (#1107, #1110) (#1122): 2 of 2 test(s) reached the change; crash, fixed by #1122 (recorded)
- [valkey-io/valkey-search#1146](https://github.com/valkey-io/valkey-search/pull/1146) ([tests](tests/valkey-search/1146-c333fb3f1/)): Fix FT.INTERNAL_UPDATE crashes during AOF/RDB load (#1108, #1109) (#1146): 3 of 3 test(s) reached the change; crash, fixed by #1146 (recorded); crash, fixed by #1146 (recorded); crash, fixed by #1146 (recorded)
- [valkey-io/valkey-search#868](https://github.com/valkey-io/valkey-search/pull/868) ([tests](tests/valkey-search/868-f1277da3e/)): Suppress core dumps around ASSERT_DEATH and EXPECT_DEATH testing calls (#868): declined: The change is confined to unit-test infrastructure: it replaces EXPECT_DEATH/ASSERT_DEATH with VMSDK_EXPECT_DEATH/VMSDK_ASSERT_DEATH macros in three test files and adds an RLimitGuard in vmsdk/src/testing_infra/utils.h that sets RLIMIT_CORE to 0 while gtest death tests run, so intentional 
- [valkey-io/valkey-search#1131](https://github.com/valkey-io/valkey-search/pull/1131) ([tests](tests/valkey-search/1131-1a43d0b5c/)): Fix reclaimable_memory underflow issue (#1131): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search#1044](https://github.com/valkey-io/valkey-search/pull/1044) ([tests](tests/valkey-search/1044-be614f3b9/)): Avoid ref counting in Text iterators + Postings ContainsFields short circuit (#1044): 0 of 4 test(s) reached the change; crash, introduced and since fixed (recorded)
- [valkey-io/valkey-search#1063](https://github.com/valkey-io/valkey-search/pull/1063) ([tests](tests/valkey-search/1063-d1b1672a2/)): Implement search.emulate_release capability as described in COMPATIBILITY.md (#1063): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1117](https://github.com/valkey-io/valkey-search/pull/1117) ([tests](tests/valkey-search/1117-ac4ced138/)): Fix GetTargetsForSlot to resolve slots in the middle/end of a range (#1117): declined: no proposed test validated
- [valkey-io/valkey-search#1119](https://github.com/valkey-io/valkey-search/pull/1119) ([tests](tests/valkey-search/1119-844be9cec/)): Fix memory reporting when reader/writer threads change at runtime (#1119): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1115](https://github.com/valkey-io/valkey-search/pull/1115) ([tests](tests/valkey-search/1115-e8a160353/)): Fix SIGSEGV on FT.SEARCH query with empty OR right-hand side (#1115): 0 of 3 test(s) reached the change
- [valkey-io/valkey-search#1026](https://github.com/valkey-io/valkey-search/pull/1026) ([tests](tests/valkey-search/1026-22788c40e/)): Implement space savings for Tag and Numeric (#1026): 0 of 2 test(s) reached the change
- [valkey-io/valkey-search#1112](https://github.com/valkey-io/valkey-search/pull/1112) ([tests](tests/valkey-search/1112-e38a5d9c4/)): Update CME error message for MULTI/EXEC and Lua (#1112): declined: no proposed test validated
- [valkey-io/valkey-search#866](https://github.com/valkey-io/valkey-search/pull/866) ([tests](tests/valkey-search/866-beea10a22/)): Refactor support for lua/multi on single slot indexes (#866): 0 of 4 test(s) reached the change
- [valkey-io/valkey-search#937](https://github.com/valkey-io/valkey-search/pull/937) ([tests](tests/valkey-search/937-5f261980c/)): Fix numeric range search doesn't accept scientific notation (#937): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1043](https://github.com/valkey-io/valkey-search/pull/1043) ([tests](tests/valkey-search/1043-8c260dba8/)): Support Borrowed InternedString Ptr to avoid ref counting  (#1043): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1019](https://github.com/valkey-io/valkey-search/pull/1019) ([tests](tests/valkey-search/1019-b668a7173/)): Fix allow_replace_deleted_ not set before LoadIndex (#1019): declined: no proposed test validated
- [valkey-io/valkey-search#1045](https://github.com/valkey-io/valkey-search/pull/1045) ([tests](tests/valkey-search/1045-b4bdc1535/)): Optimize content fetch: eliminate double key open, use HashGet for specific field access (#1045): 0 of 4 test(s) reached the change
- [valkey-io/valkey-search#1062](https://github.com/valkey-io/valkey-search/pull/1062) ([tests](tests/valkey-search/1062-fe65a492f/)): Fix ThreadPool Shutdown timing holes (#1062): declined: no proposed test validated
- [valkey-io/valkey-search#863](https://github.com/valkey-io/valkey-search/pull/863) ([tests](tests/valkey-search/863-5ee719c5c/)): Fix flakey mrmwmutex test (#863): declined: The change modifies only unit test files (vmsdk/testing/log_test.cc, mrmw_mutex_test.cc, thread_pool_test.cc): it fixes flaky test logic, initializes test-local atomics, and re-enables tests previously disabled behind BROKEN_UNIT_TEST. No source file compiled into the valkey-search module 
- [valkey-io/valkey-search#1009](https://github.com/valkey-io/valkey-search/pull/1009) ([tests](tests/valkey-search/1009-66bbdce0c/)): Fix compatibility test suite (#1009): declined: The change only modifies the project's own integration test suite (integration/compatibility/*, integration/compatibility_test.py) and .gitignore: it adds a sources-hash check for the pickle answer files, a regenerate.sh script, skips flaky tests, and regenerates the pickles. No module sou
- [valkey-io/valkey-search#1008](https://github.com/valkey-io/valkey-search/pull/1008) ([tests](tests/valkey-search/1008-2dc8e1f6a/)): Add SCORE_FIELD support and SCORE range validation to FT.CREATE (#1008): 4 of 7 test(s) reached the change, 3 skipped (required capability 'build' is unsupported)

## Totals

- Merges whose tests reached the change: 268 of 360 tested (74%)
- Changed-line coverage: median 100% of 153 merge(s) measured, 8 under 50%
- Recall: 82 parents flagged of 106 fix merges
- Claude: $1905.87 in the last 24 hours, $2382.99 in the last 7 days, $2382.99 in all
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
