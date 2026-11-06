# Fuzzer walk

Updated 2026-11-06T16:36+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 381 | 2117 | #3465 (2026-08-05) | 224 | 54.4 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 1 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 255 | 413 | #903 (2026-03-13) | 178 | 36.4 |

## Last runs

### valkey-io/valkey

[2026-11-06T16:26:03+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42227216893): 20 merge(s) tested, 3 without a code change.

- [valkey-io/valkey#3971](https://github.com/valkey-io/valkey/pull/3971) ([tests](tests/valkey/3971-8eb992eb0/)): Fix GEORADIUS STORE ACL bypass via duplicate options (#3971): 2 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4359](https://github.com/valkey-io/valkey/pull/4359) ([tests](tests/valkey/4359-a34bb4c8b/)): Replace the ZSET skiplist with a B+ tree behind an ordered index (#4359): 14 of 18 test(s) reached the change, coverage 76%
- [valkey-io/valkey#4342](https://github.com/valkey-io/valkey/pull/4342) ([tests](tests/valkey/4342-ee7f28805/)): Fix malformed AOF after a short read in MULTI (#4342): declined: no proposed test validated
- [valkey-io/valkey#3973](https://github.com/valkey-io/valkey/pull/3973) ([tests](tests/valkey/3973-00e907878/)): Clear import-source connection flag on RESET (#3973): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4351](https://github.com/valkey-io/valkey/pull/4351) ([tests](tests/valkey/4351-ea9738ab7/)): Fix NULL deref in usUntilEarliestTimer when all time events are deleted (#4351): 2 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4383](https://github.com/valkey-io/valkey/pull/4383) ([tests](tests/valkey/4383-0afdf6a4d/)): Tests: Add subject alternative names to the TLS test certificates (#4383): declined: The change modifies only test infrastructure: tests/unit/tls.tcl (the TCL test suite's TLS client setup) and utils/gen-test-certs.sh (the script that generates self-signed test certificates with added subjectAltName extensions). No Valkey server, command, replication, persistence, or tool 
- [valkey-io/valkey#4272](https://github.com/valkey-io/valkey/pull/4272) ([tests](tests/valkey/4272-8abd07fff/)): Fix slow-clocksource check for HW monotonic clock and non-x86 advisories (#4272): declined: The entire diff is confined to checkClocksource() in src/syscheck.c, which is only executed by the 'valkey-server --check-system' diagnostic subcommand (server.c:7611). It is never called during normal server startup, by any command, or by any of the four tools valkey.action.tools can run 
- [valkey-io/valkey#4376](https://github.com/valkey-io/valkey/pull/4376) ([tests](tests/valkey/4376-2fa20454a/)): Fix re-entry into processPendingCommandAndInputBuffer on blocked clients (#4376): declined: no proposed test validated
- [valkey-io/valkey#2204](https://github.com/valkey-io/valkey/pull/2204) ([tests](tests/valkey/2204-fd3d2f929/)): Add cluster-replica-priority to allow better ranking in auto failover (#2204): 4 of 7 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4215](https://github.com/valkey-io/valkey/pull/4215) ([tests](tests/valkey/4215-be0a7cbd2/)):  valkey-benchmark: reject empty command sequence before connecting (#4215): 1 of 1 test(s) reached the change, coverage 93%; [crash, appeared after #4215](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/81)
- [valkey-io/valkey#4307](https://github.com/valkey-io/valkey/pull/4307) ([tests](tests/valkey/4307-8ee7b372f/)): Fix VLA stack overflow in TLS writev after write failure (#4307): declined: The only behavioral change is the fix to the VLA stack overflow in src/tls.c connTLSWritev, reachable solely on a TLS client connection after a forced TLS write failure followed by a large scatter/gather reply (connTLSWritev is the TLS connection type's writev; plain/unix connections use t
- [valkey-io/valkey#4346](https://github.com/valkey-io/valkey/pull/4346) ([tests](tests/valkey/4346-dad4b1e66/)): Fix frozen monotonic clock on unsynchronised TSC hosts (#4346): declined: The change is confined to one-time TSC calibration in monotonicInit_x86linux() at process startup. Its new branches fire only when the host's TSC reads backwards or a calibration sample is degenerate, i.e. on hardware with unsynchronized TSCs. On the fuzzer's hosts (synchronized constant T
- [valkey-io/valkey#2891](https://github.com/valkey-io/valkey/pull/2891) ([tests](tests/valkey/2891-2d69e5e9b/)): Reset prefetch and ACL stats via CONFIG RESETSTAT (#2891): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4212](https://github.com/valkey-io/valkey/pull/4212) ([tests](tests/valkey/4212-b2fb0e13f/)): Fix/ready key blocked client uaf (#4212): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4161](https://github.com/valkey-io/valkey/pull/4161) ([tests](tests/valkey/4161-49d7d96fb/)): Optimize exact XTRIM MAXLEN zero (#4161): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4152](https://github.com/valkey-io/valkey/pull/4152) ([tests](tests/valkey/4152-114e415b6/)): Account for deferred client frees during eviction (#4152): 0 of 4 test(s) reached the change, coverage 22%
- [valkey-io/valkey#3956](https://github.com/valkey-io/valkey/pull/3956) ([tests](tests/valkey/3956-28aa048e2/)): Fix garbage earliest-expiry read in vsetEstimatedEarliestExpiry RAX path (#3956): declined: The fix touches only vsetEstimatedEarliestExpiry() in src/vset.c, and a repository-wide search shows that function has no callers outside its own declaration (src/vset.h:91) and the new unit test (src/unit/test_vset.cpp): the hash-field expiry paths (vsetRemoveExpired via hashTypeDeleteExp
- [valkey-io/valkey#4341](https://github.com/valkey-io/valkey/pull/4341) ([tests](tests/valkey/4341-1050daf65/)): Add unit test coverage for vsetMemUsage(), vsetClear() and vsetIsValid() (#4341): declined: The change only adds C++ unit tests (test_vset.cpp) and a build-system tweak restricting -fno-var-tracking-assignments to GCC for the unit-test target. No server, replication, persistence, command, or tool code changes: there is no runtime behaviour a scenario could exercise or compare.
- [valkey-io/valkey#4312](https://github.com/valkey-io/valkey/pull/4312) ([tests](tests/valkey/4312-26f44a2e1/)): Fix signed overflow in vset bucket-timestamp rounding  (#4312): 1 of 2 test(s) reached the change, coverage 90%; crash, fixed by #4312 (recorded)
- [valkey-io/valkey#4091](https://github.com/valkey-io/valkey/pull/4091) ([tests](tests/valkey/4091-21c0d49a0/)): Fix crash when processing heartbeat from a node with no local role (#4091): 0 of 6 test(s) reached the change, coverage 35%

### valkey-io/valkey-bloom

[2026-11-06T16:35:33+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42253719537): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-06T16:36:25+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42253836742): 0 merge(s) tested, 0 without a code change.


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

- Merges whose tests reached the change: 315 of 430 tested (73%)
- Changed-line coverage: median 100% of 195 merge(s) measured, 10 under 50%
- Recall: 93 parents flagged of 129 fix merges
- Claude: $1639.15 in the last 24 hours, $2901.58 in the last 7 days, $2901.58 in all
- Issues filed: 35
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

## Retries

None.
