# Fuzzer walk

Updated 2026-11-07T16:36+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 670 | 1837 | #3352 (2026-03-11) | 289 | 95.7 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 435 | 234 | #401 (2025-11-21) | 180 | 62.1 |

## Last runs

### valkey-io/valkey

[2026-11-07T15:57:00+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42373181380): 20 merge(s) tested, 13 without a code change.

- [valkey-io/valkey#2936](https://github.com/valkey-io/valkey/pull/2936) ([tests](tests/valkey/2936-644471751/)): Module command result callback addition (#2936): 0 of 6 test(s) reached the change, coverage 30%; [check, predates #2936](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/92)
- [valkey-io/valkey#3366](https://github.com/valkey-io/valkey/pull/3366) ([tests](tests/valkey/3366-fb655dbf5/)): Replace dict with thin wrapper around hashtable (#3366): 0 of 21 test(s) reached the change, coverage 69%; [crash, appeared after #3366](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/94); crash, predates #3366, since fixed (recorded)
- [valkey-io/valkey#2811](https://github.com/valkey-io/valkey/pull/2811) ([tests](tests/valkey/2811-8bb8d9168/)): Enhance cluster stale packet detection to prevent sub-replica and empty primary (#2811): 0 of 3 test(s) reached the change, coverage 100%, 1 skipped (required capability 'save-behavior-support' is unsupported)
- [valkey-io/valkey#3360](https://github.com/valkey-io/valkey/pull/3360) ([tests](tests/valkey/3360-95860937d/)): Optimize WATCH duplicate key check from O(N) to O(1) using per-db hashtable (#3360): 0 of 5 test(s) reached the change, coverage 82%; [check, predates #3360](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/93)
- [valkey-io/valkey#3421](https://github.com/valkey-io/valkey/pull/3421): to retry, 1 of 25 run(s) could not start: the run could not start: runner execution failed during deployment.provision: LocalDeploymentError: the 10 cluster nodes did not all learn of each other within 30.00s o
- [valkey-io/valkey#3416](https://github.com/valkey-io/valkey/pull/3416) ([tests](tests/valkey/3416-cf05c0d28/)): fix test_entry to consider diffrerent allocator size classes (#3416): declined: The change modifies only the unit test file src/unit/test_entry.cpp: it skips the entryUpdate gtest when jemalloc is not the allocator and lengthens two test strings so the test's size-class assumptions hold. No runtime server, command, replication, persistence, or tool behavior changes, s
- [valkey-io/valkey#3419](https://github.com/valkey-io/valkey/pull/3419) ([tests](tests/valkey/3419-edce58c93/)): Improve listpack threshold guidance in valkey.conf (#3419): declined: The change only edits comment text in valkey.conf: it rewords the guidance around the listpack/intset threshold directives (hash-max-listpack-*, list-max-listpack-size, list-compress-depth, set-max-intset-entries, set-max-listpack-*, zset-max-listpack-*). All directive names and default va
- [valkey-io/valkey#2227](https://github.com/valkey-io/valkey/pull/2227) ([tests](tests/valkey/2227-6822a676f/)): Do the failover immediately if the replica is the best ranked replica (#2227): 0 of 4 test(s) reached the change, coverage 93%, 2 skipped (required capability 'save-behavior-support' is unsupported)
- [valkey-io/valkey#3033](https://github.com/valkey-io/valkey/pull/3033) ([tests](tests/valkey/3033-60c1b0b86/)): ARM NEON SIMD optimization for pvFind() in vset.c (#3033): 0 of 3 test(s) reached the change, coverage 85%
- [valkey-io/valkey#3398](https://github.com/valkey-io/valkey/pull/3398) ([tests](tests/valkey/3398-f7a64c550/)): Fix EntryTest.entryUpdate failure on macOS due to allocator differences (#3398): declined: The change modifies only src/unit/test_entry.cpp, a unit test: it shortens test string values so EntryTest.entryUpdate's assumptions about embedded-entry allocation sizes (the 3/4 threshold cases) hold under macOS's allocator as well as jemalloc, and fixes a comment numbering typo. No serv
- [valkey-io/valkey@78ee1f555d11](https://github.com/valkey-io/valkey/commit/78ee1f555d118907bf8fe0e547a4f7d43e0d521b) ([tests](tests/valkey/78ee1f555/)): ci: include gtests in code coverage report: declined: This is a pure CI/build change: it installs googletest in the codecov GitHub Actions workflow and makes the `lcov` Makefile target also run the gtest unit tests with coverage instrumentation before generating the coverage report. No server, command, replication, persistence, or tool code p
- [valkey-io/valkey#3209](https://github.com/valkey-io/valkey/pull/3209) ([tests](tests/valkey/3209-3c46f667b/)): Fix `valkey-cli --cluster del-node` for unreachable nodes (#3209): declined: The change is confined to valkey-cli's cluster manager (--cluster del-node/add-node handling of unreachable or empty members) and only manifests on multi-node clusters containing an unreachable or empty-but-reachable node. The sole installed valkey-cli driver, valkey.action.tools, can only
- [valkey-io/valkey#3276](https://github.com/valkey-io/valkey/pull/3276) ([tests](tests/valkey/3276-9000e26ec/)): Pin workflow pip/go/npm dependencies for OpenSSF compliance (#3276): declined: The change pins pip/go/npm dependency versions in GitHub Actions workflow files (.github/workflows/*.yml) and the reply-schema-linter's package.json/package-lock.json for OpenSSF compliance. It is purely a CI/build-infrastructure change: no Valkey server code, command behaviour, replicatio
- [valkey-io/valkey#3380](https://github.com/valkey-io/valkey/pull/3380) ([tests](tests/valkey/3380-cd0b4c5e8/)): CLUSTERSCAN MATCH pattern maps to a specific slot optimizations (#3380): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3375](https://github.com/valkey-io/valkey/pull/3375) ([tests](tests/valkey/3375-543a6b83d/)): Ensure the daily workflow uses gtest-parallel to run unit tests in isolation (#3375): declined: The change is purely CI and build infrastructure: it updates .github/workflows/ci.yml and daily.yml to invoke unit tests through the `make test-unit` gtest-parallel target, adds flag plumbing (accurate/large_memory/valgrind/seed, UNIT_TEST_PATTERN) to src/unit/Makefile and src/unit/CMakeLi
- [valkey-io/valkey#3379](https://github.com/valkey-io/valkey/pull/3379) ([tests](tests/valkey/3379-afe6ee11b/)): remove duplicated lline (#3379): declined: The change only removes a duplicated assignment `shared.integers[j]->encoding = OBJ_ENCODING_INT;` in createSharedObjects (src/server.c); the same value is assigned to the same field twice in a row, so removing the second assignment is a pure cleanup with no behaviour change to test.
- [valkey-io/valkey#3359](https://github.com/valkey-io/valkey/pull/3359) ([tests](tests/valkey/3359-c921cb5aa/)): Fix incorrect memory overhead calculation for watched keys (#3359): declined: The change is a one-line correctness fix to a memory-accounting constant in multiStateMemOverhead() (sizeof(watchedKey) instead of sizeof(list)), altering the per-watched-key client-memory estimate by 8 bytes per key. It flows only into getClientMemoryUsage(), i.e. the tot-mem field of CLI
- [valkey-io/valkey#3002](https://github.com/valkey-io/valkey/pull/3002) ([tests](tests/valkey/3002-b58083d9e/)): Optimize streams range hot path (#3002): 0 of 4 test(s) reached the change, coverage 96%
- [valkey-io/valkey#3263](https://github.com/valkey-io/valkey/pull/3263) ([tests](tests/valkey/3263-c9ce3e091/)): Fix OOM aborts in large-memory ASAN tests on GitHub runners (#3263): declined: The change modifies only CI configuration and test code: .github/workflows/daily.yml gains memory monitoring and reduces test client parallelism, a quicklist gtest is skipped under ASAN, and the large-memory TCL tests reduce payload sizes (5GB to ~4.3GB, still above the 2^32 boundary they 
- [valkey-io/valkey#1901](https://github.com/valkey-io/valkey/pull/1901) ([tests](tests/valkey/1901-4cbec4a1d/)): Reuse RDB file as AOF preamble on replica after disk-based full sync (#1901): 0 of 10 test(s) reached the change, coverage 63%

### valkey-io/valkey-bloom

[2026-11-07T16:05:44+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42389482100): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-07T16:36:16+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42392077108): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-07T13:32:51+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42369561194): 20 merge(s) tested, 2 without a code change.

- [valkey-io/valkey-search#536](https://github.com/valkey-io/valkey-search/pull/536) ([tests](tests/valkey-search/536-f06b8b2ca/)): ignore venv (#536): declined: The change only adds venv/, env/, .venv/ and .env/ entries to .gitignore. It is a repository housekeeping change with no code, build, or runtime behaviour change in the valkey-search module, so there is nothing a scenario could exercise or compare.
- [valkey-io/valkey-search#533](https://github.com/valkey-io/valkey-search/pull/533) ([tests](tests/valkey-search/533-3f444fae0/)): Feature/ft create sortable (#533): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#531](https://github.com/valkey-io/valkey-search/pull/531) ([tests](tests/valkey-search/531-4da6e969e/)): refresh cluster map in background (#531): declined: The change adds one branch in ValkeySearch::OnServerCronCallback that refreshes the cluster map in the background, gated on IsCluster() && UsingCoordinator(). UsingCoordinator() is true only when the hidden, startup-only 'use-coordinator' config is set, and the only mechanism that sets it 
- [valkey-io/valkey-search#528](https://github.com/valkey-io/valkey-search/pull/528) ([tests](tests/valkey-search/528-c3fa174e8/)): Change default Valkey to 9.0.1 release (#528): declined: The change is a CI version bump (scripts/common.rc, testing/integration/run.sh set default Valkey to 9.0.1) plus raising kMinimumServerVersion in src/version.h from 8.1.1 to 9.0.1. That constant is read only once, in vmsdk/src/module.cc's load-time gate: if the server version is below it, 
- [valkey-io/valkey-search#527](https://github.com/valkey-io/valkey-search/pull/527) ([tests](tests/valkey-search/527-ed0648404/)): Fix Aggregation processing of LOAD clause and validation of vector query parameters. (#527): 2 of 2 test(s) reached the change; crash, predates #527, since fixed (recorded)
- [valkey-io/valkey-search#524](https://github.com/valkey-io/valkey-search/pull/524) ([tests](tests/valkey-search/524-31d37d91d/)): use negate flag parser in ft.info parser (#524): declined: Pure refactor with no behaviour change: the diff replaces two inline lambda parsers for FT.INFO's ALLSHARDS and INCONSISTENT keywords with the GENERATE_NEGATED_FLAG_PARSER macro, which expands (vmsdk/src/command_parser.h:211-216) to an identical lambda that sets the same field (enable_part
- [valkey-io/valkey-search#510](https://github.com/valkey-io/valkey-search/pull/510) ([tests](tests/valkey-search/510-990efa0ee/)): Fix replica crash by using legacy ACL path during replication (#510): 0 of 1 test(s) reached the change
- [valkey-io/valkey-search#410](https://github.com/valkey-io/valkey-search/pull/410) ([tests](tests/valkey-search/410-fd9d5ea4b/)): Full support for DB numbers other than zero in cluster mode. (#410): 4 of 4 test(s) reached the change; crash, introduced and since fixed (recorded); crash, fixed by #410 (recorded); crash, fixed by #410 (recorded); crash, fixed by #410 (recorded)
- [valkey-io/valkey-search#522](https://github.com/valkey-io/valkey-search/pull/522) ([tests](tests/valkey-search/522-6706ea268/)): Limit and Nocontent support for non vector queries (#522): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#469](https://github.com/valkey-io/valkey-search/pull/469) ([tests](tests/valkey-search/469-b81574a20/)): FT.INFO partition and consistency controls (#469): 3 of 3 test(s) reached the change; crash, predates #469, since fixed (recorded)
- [valkey-io/valkey-search#464](https://github.com/valkey-io/valkey-search/pull/464) ([tests](tests/valkey-search/464-0026dfcf1/)): FT.SEARCH partition and consistency controls (#464): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#299](https://github.com/valkey-io/valkey-search/pull/299) ([tests](tests/valkey-search/299-c50f8413a/)): feat: set command info for ft.create (#299): 2 of 2 test(s) reached the change; crash, introduced and since fixed (recorded); crash, fixed by #299 (recorded); crash, fixed by #299 (recorded)
- [valkey-io/valkey-search#496](https://github.com/valkey-io/valkey-search/pull/496) ([tests](tests/valkey-search/496-de6457112/)): Fix Override Specifier and Suppress MacOS Compiler Warning (#496): declined: The change modifies only CMakeLists.txt: it reformats the SAN_BUILD cache variable and sanitizer messages, and adds -Wno-defaulted-function-deleted on Apple builds to suppress a macOS compiler warning. The diff is complete (diff_truncated: false) and contains no C++ source changes, so no r
- [valkey-io/valkey-search#503](https://github.com/valkey-io/valkey-search/pull/503) ([tests](tests/valkey-search/503-d09a78fc9/)): Fix warnings and formatting (#503): declined: The change is a warnings-and-formatting cleanup with no behaviour change. The only production-code edit adds `default: CHECK(false)` to the KeyAccessToStringView switch in src/acl.cc, which is unreachable: acl::KeyAccess has exactly two enumerators (kRead, kWrite) and every call site passe
- [valkey-io/valkey-search#480](https://github.com/valkey-io/valkey-search/pull/480) ([tests](tests/valkey-search/480-57e5dfde0/)): Populate fingerprint and version to IndexSchema (#480): declined: Every behavioral change sits behind the coordinator, which only the module load-time argument --use-coordinator can enable: the 'use-coordinator' config is built Hidden (valkey_search_options.cc:129) and ModuleConfigManager::Init skips hidden entries, so it is not registered with Valkey's 
- [valkey-io/valkey-search#428](https://github.com/valkey-io/valkey-search/pull/428) ([tests](tests/valkey-search/428-cad62657f/)): Implement queue wait time aware local node preference in fanout operations (#428): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#495](https://github.com/valkey-io/valkey-search/pull/495) ([tests](tests/valkey-search/495-be8b2368f/)): Fix flaky save/restore test (#495): declined: The change is a test fix plus diagnostics: the integration test gains a wait for index backfill after restart (test code only), log.cc loses a blank line, and module OnLoad gains a read-only loop over /proc/self/maps that logs each loaded .so's base address at NOTICE level to make addr2lin
- [valkey-io/valkey-search#494](https://github.com/valkey-io/valkey-search/pull/494): to retry, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey-search#479](https://github.com/valkey-io/valkey-search/pull/479): to retry, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey-search#492](https://github.com/valkey-io/valkey-search/pull/492) ([tests](tests/valkey-search/492-d90a35802/)): fix mem leak in unit test (#492): declined: The change touches only testing/valkey_search_test.cc, the module's own gtest unit test: it replaces a raw heap allocation of a mock CLUSTER SLOTS reply with a std::unique_ptr so the mock reply is freed when the test doesn't delete it, fixing a leak in the test harness itself. No shipped m

## Totals

- Merges whose tests reached the change: 442 of 648 tested (68%)
- Changed-line coverage: median 100% of 306 merge(s) measured, 19 under 50%
- Recall: 130 parents flagged of 193 fix merges
- Claude: $1747.37 in the last 24 hours, $4648.95 in the last 7 days, $4648.95 in all
- Issues filed: 47
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

## Retries

- [valkey-io/valkey-search#494](https://github.com/valkey-io/valkey-search/pull/494): attempt 1 of 3, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey-search#479](https://github.com/valkey-io/valkey-search/pull/479): attempt 1 of 3, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey#3800](https://github.com/valkey-io/valkey/pull/3800): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#3613](https://github.com/valkey-io/valkey/pull/3613): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#3498](https://github.com/valkey-io/valkey/pull/3498): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#3421](https://github.com/valkey-io/valkey/pull/3421): attempt 1 of 3, 1 of 25 run(s) could not start: the run could not start: runner execution failed during deployment.provision: LocalDeploymentError: the 10 cluster nodes did not all learn of each other within 30.00s o
