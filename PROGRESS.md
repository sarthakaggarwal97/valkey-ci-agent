# Fuzzer walk

Updated 2026-11-06T09:45+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 312 | 2184 | #4409 (2026-08-27) | 228 | 44.6 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 81 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 196 | 472 | #1059 (2026-05-22) | 172 | 28.0 |

## Last runs

### valkey-io/valkey

[2026-11-06T09:16:35+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42174511407): 20 merge(s) tested, 1 without a code change.

- [valkey-io/valkey#4076](https://github.com/valkey-io/valkey/pull/4076) ([tests](tests/valkey/4076-88a536033/)): QoS for system critical events ( such as cluster heartbeats, replication streams, and slot migration) (#4076): 5 of 30 test(s) reached the change, coverage 77%
- [valkey-io/valkey@ed9dc9fa82c6](https://github.com/valkey-io/valkey/commit/ed9dc9fa82c6134bb6c14e73ba20966e92441fcd) ([tests](tests/valkey/ed9dc9fa8/)): Count dual-channel rdb-channel bytes in replication input stats: 0 of 3 test(s) reached the change, coverage 100%; check, fixed by ed9dc9fa82c6 (recorded)
- [valkey-io/valkey#3438](https://github.com/valkey-io/valkey/pull/3438): Cluster Bus IO offload (#3438): error: 4 of 62 run(s) could not start: the run could not start: runner execution failed during deployment.provision: LocalDeploymentError: the cluster deployment was not healthy within 30s: the nodes disagree (valkey-0: ok, 16384/16384, fail=0; valkey-1: ok, 16384/16384, fail=0; valkey-2: ok, 16384/
- [valkey-io/valkey#4460](https://github.com/valkey-io/valkey/pull/4460) ([tests](tests/valkey/4460-acba5ddd0/)): Forkless Save (#4460): 2 of 16 test(s) reached the change, coverage 62%; crash, introduced and since fixed (recorded)
- [valkey-io/valkey#4356](https://github.com/valkey-io/valkey/pull/4356) ([tests](tests/valkey/4356-1cca6d619/)): Introduce steady-state replication throttling and throttling infrastructure (#4356): 2 of 9 test(s) reached the change, coverage 85%
- [valkey-io/valkey#3708](https://github.com/valkey-io/valkey/pull/3708) ([tests](tests/valkey/3708-a1471664f/)): Add server-side hot key detection (#3708): 3 of 7 test(s) reached the change, coverage 93%
- [valkey-io/valkey#3105](https://github.com/valkey-io/valkey/pull/3105) ([tests](tests/valkey/3105-9ab664158/)): SET: add IFNE conditional option (#3105): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4533](https://github.com/valkey-io/valkey/pull/4533) ([tests](tests/valkey/4533-47b497e4e/)): Fix stale MY_PRIMARY_FAIL gossip flag (#4533): 0 of 6 test(s) reached the change, coverage 83%
- [valkey-io/valkey#4571](https://github.com/valkey-io/valkey/pull/4571) ([tests](tests/valkey/4571-a9963f495/)): Bump fast-uri to 3.1.6 in reply-schema-linter to clear CVEs (#4571): declined: The change only bumps the fast-uri npm dependency (3.1.0 to 3.1.6) in utils/reply-schema-linter/package-lock.json, a Node.js lint tool run in CI to validate command reply schemas. No Valkey server code, command behaviour, replication/persistence path, or shipped tool (valkey-cli, valkey-be
- [valkey-io/valkey#3833](https://github.com/valkey-io/valkey/pull/3833) ([tests](tests/valkey/3833-f2c1c5e9d/)): Speed up split-vote elections with the new FAILOVER_AUTH_NACK message (#3833): 1 of 6 test(s) reached the change, coverage 82%
- [valkey-io/valkey#2555](https://github.com/valkey-io/valkey/pull/2555) ([tests](tests/valkey/2555-f1addd8f3/)): Use BIO thread for cluster config saving in cluster-config-save-behavior best-effort mode (#2555): 2 of 6 test(s) reached the change, coverage 86%
- [valkey-io/valkey#4169](https://github.com/valkey-io/valkey/pull/4169) ([tests](tests/valkey/4169-8d34678c8/)): Add per-direction cluster link established counter info fields (#4169): 2 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4577](https://github.com/valkey-io/valkey/pull/4577) ([tests](tests/valkey/4577-279b07f9e/)): Compare the certificate CN over its whole length (#4577): declined: The change only alters tlsGetPeerUser() CN matching for TLS client-certificate authentication (tls-auth-clients-user CN): it compares the certificate CN over its full length so an embedded NUL can no longer truncate 'Client-only\0attacker' into the user 'Client-only'. Exercising it require
- [valkey-io/valkey#4539](https://github.com/valkey-io/valkey/pull/4539) ([tests](tests/valkey/4539-8755cb156/)): Remove the static compression buffer, over-allocate instead (#4539): 0 of 6 test(s) reached the change, coverage 100%, 1 skipped (required capability 'build' is unsupported)
- [valkey-io/valkey#4576](https://github.com/valkey-io/valkey/pull/4576) ([tests](tests/valkey/4576-637f716b4/)): Add missing UPDATE key spec for HGETEX command (#4576): 2 of 2 test(s) reached the change
- [valkey-io/valkey#4574](https://github.com/valkey-io/valkey/pull/4574) ([tests](tests/valkey/4574-7be7d564f/)): Fix script debugger uaf (#4574): 4 of 4 test(s) reached the change, coverage 100%; [check, introduced by #4574](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/75); crash, fixed by #4574 (recorded); [crash, appeared after #4574](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/61); [check, appeared after #4574](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/76); check, introduced and since fixed (recorded); crash, fixed by #4574 (recorded); check, introduced and since fixed (recorded)
- [valkey-io/valkey#4559](https://github.com/valkey-io/valkey/pull/4559) ([tests](tests/valkey/4559-597114d0b/)): Avoid offloading ACK's reads to IO threads for the slot migration export job while snapshotting (#4559): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4446](https://github.com/valkey-io/valkey/pull/4446) ([tests](tests/valkey/4446-e23ccb31d/)): Add ACL DIGEST command (#4446): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4534](https://github.com/valkey-io/valkey/pull/4534) ([tests](tests/valkey/4534-75215a56c/)): Fix RDMA UAF: connection freed inside callHandler (#4534): declined: The change is confined to src/rdma.c (a use-after-free fix in the RDMA connection type: callHandler return-value check in connRdmaEventHandler, unlinking from pending_list in connRdmaClose, and iterator-safe traversal in rdmaProcessPendingData). This code only runs when Valkey is built wit
- [valkey-io/valkey#4531](https://github.com/valkey-io/valkey/pull/4531) ([tests](tests/valkey/4531-11a74cf24/)): Fix deadlock when a blocking command is followed by a partial pipelined command (#4531): 0 of 3 test(s) reached the change, coverage 66%

### valkey-io/valkey-bloom

[2026-11-06T09:45:54+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42197417187): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-06T09:45:26+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42197357598): 0 merge(s) tested, 0 without a code change.


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

- Merges whose tests reached the change: 279 of 376 tested (74%)
- Changed-line coverage: median 100% of 168 merge(s) measured, 8 under 50%
- Recall: 83 parents flagged of 109 fix merges
- Claude: $1954.42 in the last 24 hours, $2516.44 in the last 7 days, $2516.44 in all
- Issues filed: 31
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

## Retries

None.
