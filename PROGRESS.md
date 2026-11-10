# Fuzzer walk

Updated 2026-11-10T19:56+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 1584 | 937 | #1324 (2024-12-10) | 309 | 226.3 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 63 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 9.0 |
| valkey-io/valkey-search | 672 | 0 | f8106e72e984 (2024-11-27) | 3 | 96.0 |

## Last runs

### valkey-io/valkey

[2026-11-10T19:49:27+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42857906082): 20 merge(s) tested, 7 without a code change.

- [valkey-io/valkey#1451](https://github.com/valkey-io/valkey/pull/1451) ([tests](tests/valkey/1451-e203ca35b/)): Fix undefined behavior defined by ASAN (#1451): declined: The change is a pure undefined-behavior cleanup: every hunk replaces a function-pointer cast (calling e.g. sdsfree or streamFreeCG through an incompatible void(*)(void*) cast) with a trivially equivalent void*-typed wrapper that calls the same free function on the same object. I verified e
- [valkey-io/valkey#1446](https://github.com/valkey-io/valkey/pull/1446) ([tests](tests/valkey/1446-b66698b88/)): Discover and fix new typos (#1446): declined: The change only fixes typos: comments in geohash_helper.c and zmalloc.c, a CI tool version bump, Tcl test-suite variable/proc renames, and a C identifier rename (shared.special_asterick -> special_asterisk) whose object is created with the same value "*" and used in the same call site, so 
- [valkey-io/valkey#1303](https://github.com/valkey-io/valkey/pull/1303) ([tests](tests/valkey/1303-ba25b586d/)): Introduce FORCE_DEFRAG compilation option to allow activedefrag run when allocator is not jemalloc (#1303): declined: The change introduces a DEBUG_FORCE_DEFRAG compile-time option: all new runtime code (the defrag allocator stubs, HAVE_DEFRAG definition, and the activedefrag default of 1) is compiled only when that flag is passed at build time, which the fuzzer's standard builds of both commits never do.
- [valkey-io/valkey#1379](https://github.com/valkey-io/valkey/pull/1379) ([tests](tests/valkey/1379-7892bf808/)): Fix test_reclaimFilePageCache to avoid tmpfs (#1379): declined: The change modifies only src/unit/test_util.c, adding a statfs check so the unit test test_reclaimFilePageCache skips itself based on whether /tmp is tmpfs (where posix_fadvise page-cache reclaim can't be observed). It is a test-infrastructure/CI fix with no change to any server, command, 
- [valkey-io/valkey#1436](https://github.com/valkey-io/valkey/pull/1436) ([tests](tests/valkey/1436-e024b4bd2/)): Drop the MEET packet if the link node is in handshake state (#1436): 0 of 2 test(s) reached the change, coverage 40%
- [valkey-io/valkey#1356](https://github.com/valkey-io/valkey/pull/1356) ([tests](tests/valkey/1356-ad2422068/)): Automatic failover vote is not limited by two times the node timeout (#1356): 0 of 3 test(s) reached the change
- [valkey-io/valkey#1176](https://github.com/valkey-io/valkey/pull/1176) ([tests](tests/valkey/1176-88942c8e6/)): Replace dict with new hashtable for sets datatype (#1176): 5 of 17 test(s) reached the change, coverage 83%
- [valkey-io/valkey#1442](https://github.com/valkey-io/valkey/pull/1442) ([tests](tests/valkey/1442-3cd176dc3/)): Avoid importing memory aligned malloc (#1442): declined: The change only defines __MM_MALLOC_H before including immintrin.h in hyperloglog.c to suppress the declaration of the unused _mm_malloc/_mm_free aligned-allocation functions. It is a compile-time/header-hygiene change with no modification to any executed code path: the AVX2 HyperLogLog im
- [valkey-io/valkey#1431](https://github.com/valkey-io/valkey/pull/1431) ([tests](tests/valkey/1431-b60097ba0/)): Check length before reading in `stringmatchlen` (#1431): 0 of 2 test(s) reached the change, coverage 80%
- [valkey-io/valkey#1430](https://github.com/valkey-io/valkey/pull/1430) ([tests](tests/valkey/1430-32f2c73cb/)): defrag: eliminate persistent kvstore pointer and edge case fixes (#1430): 0 of 3 test(s) reached the change, coverage 81%
- [valkey-io/valkey#1429](https://github.com/valkey-io/valkey/pull/1429) ([tests](tests/valkey/1429-3a1043a4f/)): Fix Valkey binary build workflow, version support changes. (#1429): declined: The change modifies only CI/build infrastructure: .github/workflows/call-build-linux-x86-packages.yml changes the GitHub Actions runner label, and utils/releasetools/build-config.json updates the Ubuntu distro package build matrix. No server, replication, persistence, command, or tool code
- [valkey-io/valkey#1414](https://github.com/valkey-io/valkey/pull/1414) ([tests](tests/valkey/1414-2d9240452/)): Avoid defragging scripts during EVAL command execution (#1414): declined: no proposed test validated
- [valkey-io/valkey#1307](https://github.com/valkey-io/valkey/pull/1307) ([tests](tests/valkey/1307-5f7fe9ef2/)): Send MEET packet to node if there is no inbound link to fix inconsistency when handshake timedout (#1307): 0 of 4 test(s) reached the change, coverage 90%; crash, introduced and since fixed (recorded)
- [valkey-io/valkey#1420](https://github.com/valkey-io/valkey/pull/1420) ([tests](tests/valkey/1420-0c8ad5cd3/)): defrag: allow defrag to start during AOF loading (#1420): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1421](https://github.com/valkey-io/valkey/pull/1421) ([tests](tests/valkey/1421-1acf7f71c/)): Fix memory leak in the new hashtable unittest (#1421): declined: The change is a one-line fix in src/unit/test_hashtable.c, adding free(e) to plug a memory leak in the hashtable unit test's own test code. It modifies only the unittest binary, not the server: no command, reply, replication, cluster, persistence or tool behaviour changes, so no fuzzer sce
- [valkey-io/valkey@3eb8314be6af](https://github.com/valkey-io/valkey/commit/3eb8314be6af0777e69f852b65f933dd9186d30b): to retry, 1 of 110 run(s) could not start: the run could not start: runner execution failed during deployment.provision: LocalDeploymentError: the 6 cluster nodes did not all learn of each other within 30.00s o
- [valkey-io/valkey#1065](https://github.com/valkey-io/valkey/pull/1065) ([tests](tests/valkey/1065-4efff42f0/)): Replace dict with hashtable in command tables (#1065): 0 of 13 test(s) reached the change, coverage 84%
- [valkey-io/valkey@c8ee5c2c46c5](https://github.com/valkey-io/valkey/commit/c8ee5c2c46c52bcb2b48e190a8f5131fdbcf355e) ([tests](tests/valkey/c8ee5c2c4/)): Hashtable implementation including unit tests: declined: The change only adds a new, not-yet-used data structure: src/hashtable.c/.h are added and linked into the build, but no server code includes or calls the new hashtable at this commit (only hashtable.c itself and src/unit/test_hashtable.c reference it). No command, reply, replication, persi
- [valkey-io/valkey#1408](https://github.com/valkey-io/valkey/pull/1408) ([tests](tests/valkey/1408-7e564887b/)): Set HIDDEN_CONFIG flag on events-per-io-thread (#1408): 2 of 2 test(s) reached the change
- [valkey-io/valkey#1398](https://github.com/valkey-io/valkey/pull/1398) ([tests](tests/valkey/1398-f951a1ca7/)): Add new flag in `CLIENT LIST` for import-source client (#1398): 2 of 2 test(s) reached the change, coverage 100%

### valkey-io/valkey-bloom

[2026-11-10T19:56:59+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42890262114): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-10T19:26:30+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42885200555): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-10T19:40:06+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42887381024): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 759 of 1183 tested (64%)
- Changed-line coverage: median 100% of 713 merge(s) measured, 57 under 50%
- Recall: 203 parents flagged of 322 fix merges
- Claude: $1121.10 in the last 24 hours, $8638.49 in the last 7 days, $8638.49 in all
- Issues filed: 66
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/100
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/101
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/102
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/103
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/104
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/105
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/106
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/107
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/108
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/109
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/110
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/111
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/112
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/113
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/114
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/115
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
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/90
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/91
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/92
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/93
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/94
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/95
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/96
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/98

## Retries

- [valkey-io/valkey#1948](https://github.com/valkey-io/valkey/pull/1948): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#1535](https://github.com/valkey-io/valkey/pull/1535): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey@3eb8314be6af](https://github.com/valkey-io/valkey/commit/3eb8314be6af0777e69f852b65f933dd9186d30b): attempt 1 of 3, 1 of 110 run(s) could not start: the run could not start: runner execution failed during deployment.provision: LocalDeploymentError: the 6 cluster nodes did not all learn of each other within 30.00s o
