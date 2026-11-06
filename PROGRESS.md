# Fuzzer walk

Updated 2026-11-06T11:17+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 312 | 2184 | #4409 (2026-08-27) | 228 | 44.6 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 41 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 228 | 440 | #944 (2026-03-31) | 176 | 32.6 |

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

[2026-11-06T10:47:11+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42205236698): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-06T11:17:37+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42209082163): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-06T10:19:16+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42175828478): 20 merge(s) tested, 12 without a code change.

- [valkey-io/valkey-search#1037](https://github.com/valkey-io/valkey-search/pull/1037) ([tests](tests/valkey-search/1037-da3872fc8/)): Fix Reader Thread Scaling Bottlenecks due to PausePoint Mutex (#1037): 0 of 2 test(s) reached the change
- [valkey-io/valkey-search#1040](https://github.com/valkey-io/valkey-search/pull/1040) ([tests](tests/valkey-search/1040-7cb0d02c4/)): Use protobuf Arena for fanout response deserialization (#1040): declined: no proposed test validated
- [valkey-io/valkey-search#1029](https://github.com/valkey-io/valkey-search/pull/1029) ([tests](tests/valkey-search/1029-1b85d1acc/)): Fix RunByMain shutdown leak by draining pending one-shots (#1029): declined: no proposed test validated
- [valkey-io/valkey-search#945](https://github.com/valkey-io/valkey-search/pull/945) ([tests](tests/valkey-search/945-0fafddb0d/)): Use ccache to cache build object files to improve build time (#945): declined: The change only adds ccache to the build and CI pipeline: a Dockerfile package install, GitHub Actions cache-restore steps and CCACHE_DIR/CCACHE_MAXSIZE environment variables, and CMAKE_C/CXX_COMPILER_LAUNCHER settings in CMakeLists.txt. No module source code changes, so the built module's
- [valkey-io/valkey-search#1027](https://github.com/valkey-io/valkey-search/pull/1027) ([tests](tests/valkey-search/1027-1c30dd5b3/)): Fix crash in unittests introduced in previous PR (#1027): declined: The change modifies only unit-test files (testing/expr/expr_test.cc and testing/expr/value_test.cc), switching their gtest fixtures to inherit from vmsdk::ValkeyTest to fix a crash in the project's own test harness. No runtime code of the valkey-search module changes, so the module's behav
- [valkey-io/valkey-search#963](https://github.com/valkey-io/valkey-search/pull/963) ([tests](tests/valkey-search/963-70b8c2025/)): Fix timefmt causes OOM on empty input, Issue: #962 (#963): 2 of 2 test(s) reached the change; crash, fixed by #963 (recorded)
- [valkey-io/valkey-search#1003](https://github.com/valkey-io/valkey-search/pull/1003) ([tests](tests/valkey-search/1003-dcef76238/)): Ensure fixed ordering in the serialization of the fields. This matters for Text (#1003): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1007](https://github.com/valkey-io/valkey-search/pull/1007) ([tests](tests/valkey-search/1007-cabb67073/)): docker update test (#1007): declined: The change modifies only .devcontainer/Dockerfile, dropping the software-properties-common package and the ubuntu-toolchain-r/test PPA from the development container image used for local development/CI tooling. No module source, command behavior, replication, persistence, or runtime code p
- [valkey-io/valkey-search#994](https://github.com/valkey-io/valkey-search/pull/994) ([tests](tests/valkey-search/994-0f6d44d87/)): Fix Tag Search's EntriesFetcherIterator logic to not rely on RootIterator (full tree walk) on standard cases (#994): 0 of 2 test(s) reached the change
- [valkey-io/valkey-search#1002](https://github.com/valkey-io/valkey-search/pull/1002) ([tests](tests/valkey-search/1002-ef1f0a127/)): Rename utility-worker to util-worker to adhere to 15 char name limit in linux (#1002): declined: The change only renames the utility thread pool's name prefix from 'utility-worker-' to 'util-worker-' so that the string passed to pthread_setname_np fits Linux's 15-character thread-name limit. The name is used solely as an OS-level thread label for debugging (visible in /proc and top -H
- [valkey-io/valkey-search#960](https://github.com/valkey-io/valkey-search/pull/960) ([tests](tests/valkey-search/960-f1db9edb2/)): Fix FT.SEARCH bare wildcard execution (#960): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search#816](https://github.com/valkey-io/valkey-search/pull/816) ([tests](tests/valkey-search/816-7c217d43c/)): Refactor FT.AGGREGATE reducers to process records in batch (#816): 2 of 4 test(s) reached the change
- [valkey-io/valkey-search#777](https://github.com/valkey-io/valkey-search/pull/777) ([tests](tests/valkey-search/777-63574f75e/)): Fix build script for ASAN integration tests (#777): declined: The change modifies only build.sh and integration/run.sh in valkey-search's CI/test harness: it propagates pytest's exit code to the build script, captures pytest output to a log file, and scans that log for sanitizer errors in ASAN runs. No module source code, command behavior, replies, i
- [valkey-io/valkey-search#961](https://github.com/valkey-io/valkey-search/pull/961) ([tests](tests/valkey-search/961-153e037b4/)): fix test_dropindex_with_background_queries heap-use-after-free leak (#961): 0 of 2 test(s) reached the change; [crash, shown by ASan on unstable, the one build with it](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/78)
- [valkey-io/valkey-search#974](https://github.com/valkey-io/valkey-search/pull/974) ([tests](tests/valkey-search/974-b69c73a3f/)): Update search_number_of_indexes while restoring (#974): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search#966](https://github.com/valkey-io/valkey-search/pull/966) ([tests](tests/valkey-search/966-deab7ef9e/)): Fix crash on empty FT.AGGREGATE expressions (#966): 2 of 2 test(s) reached the change; crash, fixed by #966 (recorded)
- [valkey-io/valkey-search#973](https://github.com/valkey-io/valkey-search/pull/973) ([tests](tests/valkey-search/973-77ca3d550/)): fix aggregate recursion depth (#973): 0 of 4 test(s) reached the change
- [valkey-io/valkey-search#958](https://github.com/valkey-io/valkey-search/pull/958) ([tests](tests/valkey-search/958-541453bb0/)): Fix heap-use-after-free in PausePoint during server shutdown for mult… (#958): declined: no proposed test validated
- [valkey-io/valkey-search#956](https://github.com/valkey-io/valkey-search/pull/956) ([tests](tests/valkey-search/956-923430d2e/)): Fix dropindex while blocked queries crash (#956): 0 of 3 test(s) reached the change
- [valkey-io/valkey-search#943](https://github.com/valkey-io/valkey-search/pull/943) ([tests](tests/valkey-search/943-da472e234/)): Fix Boolean config type mismatch (#943): 0 of 1 test(s) reached the change

## Totals

- Merges whose tests reached the change: 285 of 388 tested (73%)
- Changed-line coverage: median 100% of 168 merge(s) measured, 8 under 50%
- Recall: 87 parents flagged of 119 fix merges
- Claude: $1788.06 in the last 24 hours, $2613.50 in the last 7 days, $2613.50 in all
- Issues filed: 32
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

## Retries

None.
