# Fuzzer walk

Updated 2026-11-08T11:43+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 910 | 1597 | #2853 (2025-11-18) | 298 | 130.0 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 613 | 56 | #33 (2025-03-01) | 220 | 87.6 |

## Last runs

### valkey-io/valkey

[2026-11-08T10:25:32+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42461631861): 20 merge(s) tested, 3 without a code change.

- [valkey-io/valkey#3613](https://github.com/valkey-io/valkey/pull/3613) ([tests](tests/valkey/3613-1d7224f38/)): Fix UAF in unblockClientOnKey when reprocessed command frees the client (CVE-2026-23479) (#3613): 0 of 2 test(s) reached the change, coverage 25%
- [valkey-io/valkey#2809](https://github.com/valkey-io/valkey/pull/2809) ([tests](tests/valkey/2809-c8341d416/)): Replace strcmp with byte-by-byte comparison in valkey-check-aof (#2809): declined: The change rewrites `strncmp(buf, "\r\n", 2) != 0` in consumeNewline of valkey-check-aof.c as `buf[0] != '\r' || buf[1] != '\n'`. The two predicates are equivalent for all byte values: strncmp against the non-NUL literal "\r\n" with n=2 returns non-zero exactly when byte 0 differs from '\r
- [valkey-io/valkey#2857](https://github.com/valkey-io/valkey/pull/2857) ([tests](tests/valkey/2857-1b5f245ea/)): Refactor of LFU/LRU code for modularity (#2857): 2 of 9 test(s) reached the change, coverage 100%, 2 skipped (required capability 'build' is unsupported: default: build rejects startup configuration: *** FATAL CONFIG FILE ERROR (Version 255.255.255) *** Reading the configuration file, at line 11 >>> 'forkless-infrastructure-enabled "yes"' Bad directive or wrong number of arguments; default deployment config); crash, introduced and since fixed (recorded); crash, fixed by #2857 (recorded)
- [valkey-io/valkey#2885](https://github.com/valkey-io/valkey/pull/2885) ([tests](tests/valkey/2885-853d111f4/)): Build: Support `make test` when PROG_SUFFIX is used (#2885): declined: The change is build/CI-only: it passes PROG_SUFFIX from `make test` into the Tcl test harness (src/Makefile and tests/support/set_executable_path.tcl) so the harness finds suffixed binary names. No Valkey server, client-tool, or protocol code changes; there is no runtime behaviour for a sc
- [valkey-io/valkey#2816](https://github.com/valkey-io/valkey/pull/2816) ([tests](tests/valkey/2816-3fd094227/)): Refactor TCL reference to support running tests with CMake (#2816): declined: The change is purely build and test infrastructure: it refactors CMake files (symlink creation, a 'hint' custom target in src/CMakeLists.txt) and the TCL test harness (tests/support/set_executable_path.tcl and sourcing it from test scripts) so the existing TCL suites can run against CMake-
- [valkey-io/valkey#2887](https://github.com/valkey-io/valkey/pull/2887) ([tests](tests/valkey/2887-825d19fb0/)): Make all ACL categories explicit in JSON files (#2887): 0 of 4 test(s) reached the change
- [valkey-io/valkey#2886](https://github.com/valkey-io/valkey/pull/2886) ([tests](tests/valkey/2886-4a0e20bbc/)): Handle failed psync log when there is no replication backlog (#2886): 1 of 4 test(s) reached the change, coverage 66%; [check, introduced by #2886](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/104); crash, fixed by #2886 (recorded)
- [valkey-io/valkey#2877](https://github.com/valkey-io/valkey/pull/2877) ([tests](tests/valkey/2877-a087cc113/)): Add psync offset range information to the failed psync log (#2877): 0 of 1 test(s) reached the change; crash, introduced and since fixed (recorded)
- [valkey-io/valkey#2881](https://github.com/valkey-io/valkey/pull/2881) ([tests](tests/valkey/2881-761fba2e9/)): Fix persisting missing make variables (#2881): declined: The change touches only src/Makefile's persist-settings target, adding PROG_SUFFIX and USE_FAST_FLOAT to the .make-settings file so that changing those build flags triggers a rebuild. It is a build-system fix with no runtime behaviour change: no command, reply, persistence, replication, or
- [valkey-io/valkey#2874](https://github.com/valkey-io/valkey/pull/2874) ([tests](tests/valkey/2874-d16788e52/)): Fix discarded-qualifiers warnings reported by fedorarawhide (#2874): declined: The change only adds const qualifiers to local pointer declarations (char * -> const char *) in cli_common.c, module.c, networking.c and resp_parser.c to silence -Wdiscarded-qualifiers warnings from fedorarawhide builds. No expression, control flow, or data layout changes; the compiled beh
- [valkey-io/valkey#2781](https://github.com/valkey-io/valkey/pull/2781) ([tests](tests/valkey/2781-faac14ab9/)): Cluster: Optimize slot bitmap iteration from per-bit to 64-bit word scan (#2781): 0 of 4 test(s) reached the change, coverage 80%, 2 skipped (required capability 'save-behavior-support' is unsupported: default: build rejects startup configuration: *** FATAL CONFIG FILE ERROR (Version 255.255.255) *** Reading the configuration file, at line 11 >>> 'cluster-config-save-behavior "sync"' Bad directive or wrong number of arguments)
- [valkey-io/valkey#2851](https://github.com/valkey-io/valkey/pull/2851) ([tests](tests/valkey/2851-56ab3c4a8/)): Adds HGETDEL Support to Valkey (#2851): 1 of 4 test(s) reached the change, coverage 93%
- [valkey-io/valkey#2875](https://github.com/valkey-io/valkey/pull/2875) ([tests](tests/valkey/2875-9562bdc0a/)): Align the complexity description for all multi field HFE commands docs (#2875): 2 of 2 test(s) reached the change
- [valkey-io/valkey#2815](https://github.com/valkey-io/valkey/pull/2815) ([tests](tests/valkey/2815-da3c43d76/)): Additional log information for cluster accept handler and message processing (#2815): 0 of 5 test(s) reached the change, coverage 75%, 2 skipped (required capability 'save-behavior-support' is unsupported: default: build rejects startup configuration: *** FATAL CONFIG FILE ERROR (Version 255.255.255) *** Reading the configuration file, at line 11 >>> 'cluster-config-save-behavior "sync"' Bad directive or wrong number of arguments)
- [valkey-io/valkey#2873](https://github.com/valkey-io/valkey/pull/2873) ([tests](tests/valkey/2873-e5de417f1/)): Fix build on 32-bit ARM by only using NEON on AArch64 (#2873): declined: The change is a one-line build fix: it tightens the HAVE_ARM_NEON preprocessor guard in src/config.h to require __aarch64__, so NEON SIMD intrinsics are only compiled on 64-bit ARM, fixing a compile failure on 32-bit ARM. It alters which code is compiled, not what any compiled build does: 
- [valkey-io/valkey#2849](https://github.com/valkey-io/valkey/pull/2849) ([tests](tests/valkey/2849-dd2827a14/)): Add support for asynchronous release to replicaKeysWithExpire on writable replica (#2849): 1 of 4 test(s) reached the change, coverage 94%
- [valkey-io/valkey#2824](https://github.com/valkey-io/valkey/pull/2824) ([tests](tests/valkey/2824-8ea7f1330/)): Update dual channel replication conf to mention the local buffer is imited by COB (#2824): declined: The change only rewords a log message in bufferReplData to include the configured byte limit, updates the test's log-matching pattern accordingly, and adds a documentation comment to valkey.conf. No executable behaviour changes: the dual-channel buffering cutoff logic, its condition, and i
- [valkey-io/valkey#2833](https://github.com/valkey-io/valkey/pull/2833) ([tests](tests/valkey/2833-8189fe5c4/)): Add rdb_transmitted to replstateToString so that we can see it in INFO (#2833): 4 of 5 test(s) reached the change, coverage 0%; [check, predates #2833](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/105)
- [valkey-io/valkey#2836](https://github.com/valkey-io/valkey/pull/2836) ([tests](tests/valkey/2836-05540af40/)): Add script function flags in the module API (#2836): 2 of 2 test(s) reached the change
- [valkey-io/valkey#2600](https://github.com/valkey-io/valkey/pull/2600) ([tests](tests/valkey/2600-3c3a1966e/)): Perform data cleanup during RDB load on successful version/signature validation (#2600): 3 of 14 test(s) reached the change, coverage 61%

### valkey-io/valkey-bloom

[2026-11-08T11:19:25+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42475049647): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-08T11:34:11+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42476284371): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-08T11:43:05+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42473044483): 20 merge(s) tested, 0 without a code change.

- [valkey-io/valkey-search#88](https://github.com/valkey-io/valkey-search/pull/88) ([tests](tests/valkey-search/88-508b0cf54/)): Adding json and flat integration tests (#88): declined: The only production change is swapping valkey-search's JSON-module detection from a JSON.GET probe to a MODULE LIST scan (IsJsonModuleLoaded -> vmsdk::IsModuleLoaded), reached only by FT.CREATE ON JSON and a one-time OnLoad log line. The swap is behavior-preserving: with the JSON module lo
- [valkey-io/valkey-search#85](https://github.com/valkey-io/valkey-search/pull/85): allow building with external libs (#85): error: `export NINJA_TOOL=ninja` failed in parent (exit 2): -- Detecting CXX compile features -- Detecting CXX compile features - done -- Performing Test CMAKE_HAVE_LIBC_PTHREAD -- Performing Test CMAKE_HAVE_LIBC_PTHREAD - Success -- Found Threads: TRUE -- Performing Test HAVE_STDC_FORMAT_MACROS -- 
- [valkey-io/valkey-search#82](https://github.com/valkey-io/valkey-search/pull/82): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 2): -- Detecting CXX compile features -- Detecting CXX compile features - done -- Performing Test CMAKE_HAVE_LIBC_PTHREAD -- Performing Test CMAKE_HAVE_
- [valkey-io/valkey-search#84](https://github.com/valkey-io/valkey-search/pull/84): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 2): -- Detecting CXX compile features -- Detecting CXX compile features - done -- Performing Test CMAKE_HAVE_LIBC_PTHREAD -- Performing Test CMAKE_HAVE_
- [valkey-io/valkey-search#75](https://github.com/valkey-io/valkey-search/pull/75): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 2): -- Detecting CXX compile features -- Detecting CXX compile features - done -- Performing Test CMAKE_HAVE_LIBC_PTHREAD -- Performing Test CMAKE_HAVE_
- [valkey-io/valkey-search#80](https://github.com/valkey-io/valkey-search/pull/80): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 2): -- Detecting CXX compile features -- Detecting CXX compile features - done -- Performing Test CMAKE_HAVE_LIBC_PTHREAD -- Performing Test CMAKE_HAVE_
- [valkey-io/valkey-search#68](https://github.com/valkey-io/valkey-search/pull/68): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 2): -- Detecting CXX compile features -- Detecting CXX compile features - done -- Performing Test CMAKE_HAVE_LIBC_PTHREAD -- Performing Test CMAKE_HAVE_
- [valkey-io/valkey-search#77](https://github.com/valkey-io/valkey-search/pull/77): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 2): -- Detecting CXX compile features -- Detecting CXX compile features - done -- Performing Test CMAKE_HAVE_LIBC_PTHREAD -- Performing Test CMAKE_HAVE_
- [valkey-io/valkey-search#76](https://github.com/valkey-io/valkey-search/pull/76): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 2): -- Detecting CXX compile features -- Detecting CXX compile features - done -- Performing Test CMAKE_HAVE_LIBC_PTHREAD -- Performing Test CMAKE_HAVE_
- [valkey-io/valkey-search#73](https://github.com/valkey-io/valkey-search/pull/73): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 2): -- Detecting CXX compile features -- Detecting CXX compile features - done -- Performing Test CMAKE_HAVE_LIBC_PTHREAD -- Performing Test CMAKE_HAVE_
- [valkey-io/valkey-search#60](https://github.com/valkey-io/valkey-search/pull/60): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 2): -- Detecting CXX compile features -- Detecting CXX compile features - done -- Performing Test CMAKE_HAVE_LIBC_PTHREAD -- Performing Test CMAKE_HAVE_
- [valkey-io/valkey-search#72](https://github.com/valkey-io/valkey-search/pull/72): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 2): -- Detecting CXX compile features -- Detecting CXX compile features - done -- Performing Test CMAKE_HAVE_LIBC_PTHREAD -- Performing Test CMAKE_HAVE_
- [valkey-io/valkey-search#71](https://github.com/valkey-io/valkey-search/pull/71): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 2): -- Detecting CXX compile features -- Detecting CXX compile features - done -- Performing Test CMAKE_HAVE_LIBC_PTHREAD -- Performing Test CMAKE_HAVE_
- [valkey-io/valkey-search#70](https://github.com/valkey-io/valkey-search/pull/70): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 2): -- Detecting CXX compile features -- Detecting CXX compile features - done -- Performing Test CMAKE_HAVE_LIBC_PTHREAD -- Performing Test CMAKE_HAVE_
- [valkey-io/valkey-search#49](https://github.com/valkey-io/valkey-search/pull/49): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 2): -- Detecting CXX compile features -- Detecting CXX compile features - done -- Performing Test CMAKE_HAVE_LIBC_PTHREAD -- Performing Test CMAKE_HAVE_
- [valkey-io/valkey-search@1d8e30e25ae6](https://github.com/valkey-io/valkey-search/commit/1d8e30e25ae640522f809f839ed2a81fb191e5c1): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 2): -- Detecting CXX compile features -- Detecting CXX compile features - done -- Performing Test CMAKE_HAVE_LIBC_PTHREAD -- Performing Test CMAKE_HAVE_
- [valkey-io/valkey-search#65](https://github.com/valkey-io/valkey-search/pull/65): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 2): -- Detecting CXX compile features -- Detecting CXX compile features - done -- Performing Test CMAKE_HAVE_LIBC_PTHREAD -- Performing Test CMAKE_HAVE_
- [valkey-io/valkey-search#64](https://github.com/valkey-io/valkey-search/pull/64): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 2): -- Detecting CXX compile features -- Detecting CXX compile features - done -- Performing Test CMAKE_HAVE_LIBC_PTHREAD -- Performing Test CMAKE_HAVE_
- [valkey-io/valkey-search#63](https://github.com/valkey-io/valkey-search/pull/63): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 2): -- Detecting CXX compile features -- Detecting CXX compile features - done -- Performing Test CMAKE_HAVE_LIBC_PTHREAD -- Performing Test CMAKE_HAVE_
- [valkey-io/valkey-search#57](https://github.com/valkey-io/valkey-search/pull/57): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 2): -- Detecting CXX compile features -- Detecting CXX compile features - done -- Performing Test CMAKE_HAVE_LIBC_PTHREAD -- Performing Test CMAKE_HAVE_

## Totals

- Merges whose tests reached the change: 577 of 843 tested (68%)
- Changed-line coverage: median 100% of 417 merge(s) measured, 32 under 50%
- Recall: 159 parents flagged of 235 fix merges
- Claude: $1780.70 in the last 24 hours, $6071.20 in the last 7 days, $6071.20 in all
- Issues filed: 56
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/100
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/101
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/102
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/103
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/104
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/105
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

- [valkey-io/valkey-search#82](https://github.com/valkey-io/valkey-search/pull/82): attempt 2 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 2): -- Detecting CXX compile features -- Detecting CXX compile features - done -- Performing Test CMAKE_HAVE_LIBC_PTHREAD -- Performing Test CMAKE_HAVE_
- [valkey-io/valkey-search#84](https://github.com/valkey-io/valkey-search/pull/84): attempt 2 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 2): -- Detecting CXX compile features -- Detecting CXX compile features - done -- Performing Test CMAKE_HAVE_LIBC_PTHREAD -- Performing Test CMAKE_HAVE_
- [valkey-io/valkey-search#75](https://github.com/valkey-io/valkey-search/pull/75): attempt 2 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 2): -- Detecting CXX compile features -- Detecting CXX compile features - done -- Performing Test CMAKE_HAVE_LIBC_PTHREAD -- Performing Test CMAKE_HAVE_
- [valkey-io/valkey-search#80](https://github.com/valkey-io/valkey-search/pull/80): attempt 2 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 2): -- Detecting CXX compile features -- Detecting CXX compile features - done -- Performing Test CMAKE_HAVE_LIBC_PTHREAD -- Performing Test CMAKE_HAVE_
- [valkey-io/valkey-search#68](https://github.com/valkey-io/valkey-search/pull/68): attempt 2 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 2): -- Detecting CXX compile features -- Detecting CXX compile features - done -- Performing Test CMAKE_HAVE_LIBC_PTHREAD -- Performing Test CMAKE_HAVE_
- [valkey-io/valkey-search#77](https://github.com/valkey-io/valkey-search/pull/77): attempt 2 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 2): -- Detecting CXX compile features -- Detecting CXX compile features - done -- Performing Test CMAKE_HAVE_LIBC_PTHREAD -- Performing Test CMAKE_HAVE_
- [valkey-io/valkey-search#76](https://github.com/valkey-io/valkey-search/pull/76): attempt 2 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 2): -- Detecting CXX compile features -- Detecting CXX compile features - done -- Performing Test CMAKE_HAVE_LIBC_PTHREAD -- Performing Test CMAKE_HAVE_
- [valkey-io/valkey-search#73](https://github.com/valkey-io/valkey-search/pull/73): attempt 2 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 2): -- Detecting CXX compile features -- Detecting CXX compile features - done -- Performing Test CMAKE_HAVE_LIBC_PTHREAD -- Performing Test CMAKE_HAVE_
- [valkey-io/valkey-search#60](https://github.com/valkey-io/valkey-search/pull/60): attempt 2 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 2): -- Detecting CXX compile features -- Detecting CXX compile features - done -- Performing Test CMAKE_HAVE_LIBC_PTHREAD -- Performing Test CMAKE_HAVE_
- [valkey-io/valkey-search#72](https://github.com/valkey-io/valkey-search/pull/72): attempt 2 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 2): -- Detecting CXX compile features -- Detecting CXX compile features - done -- Performing Test CMAKE_HAVE_LIBC_PTHREAD -- Performing Test CMAKE_HAVE_
- [valkey-io/valkey-search#71](https://github.com/valkey-io/valkey-search/pull/71): attempt 2 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 2): -- Detecting CXX compile features -- Detecting CXX compile features - done -- Performing Test CMAKE_HAVE_LIBC_PTHREAD -- Performing Test CMAKE_HAVE_
- [valkey-io/valkey-search#70](https://github.com/valkey-io/valkey-search/pull/70): attempt 2 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 2): -- Detecting CXX compile features -- Detecting CXX compile features - done -- Performing Test CMAKE_HAVE_LIBC_PTHREAD -- Performing Test CMAKE_HAVE_
- [valkey-io/valkey-search#49](https://github.com/valkey-io/valkey-search/pull/49): attempt 2 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 2): -- Detecting CXX compile features -- Detecting CXX compile features - done -- Performing Test CMAKE_HAVE_LIBC_PTHREAD -- Performing Test CMAKE_HAVE_
- [valkey-io/valkey-search@1d8e30e25ae6](https://github.com/valkey-io/valkey-search/commit/1d8e30e25ae640522f809f839ed2a81fb191e5c1): attempt 2 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 2): -- Detecting CXX compile features -- Detecting CXX compile features - done -- Performing Test CMAKE_HAVE_LIBC_PTHREAD -- Performing Test CMAKE_HAVE_
- [valkey-io/valkey-search#65](https://github.com/valkey-io/valkey-search/pull/65): attempt 2 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 2): -- Detecting CXX compile features -- Detecting CXX compile features - done -- Performing Test CMAKE_HAVE_LIBC_PTHREAD -- Performing Test CMAKE_HAVE_
- [valkey-io/valkey-search#64](https://github.com/valkey-io/valkey-search/pull/64): attempt 2 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 2): -- Detecting CXX compile features -- Detecting CXX compile features - done -- Performing Test CMAKE_HAVE_LIBC_PTHREAD -- Performing Test CMAKE_HAVE_
- [valkey-io/valkey-search#63](https://github.com/valkey-io/valkey-search/pull/63): attempt 2 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 2): -- Detecting CXX compile features -- Detecting CXX compile features - done -- Performing Test CMAKE_HAVE_LIBC_PTHREAD -- Performing Test CMAKE_HAVE_
- [valkey-io/valkey-search#57](https://github.com/valkey-io/valkey-search/pull/57): attempt 2 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 2): -- Detecting CXX compile features -- Detecting CXX compile features - done -- Performing Test CMAKE_HAVE_LIBC_PTHREAD -- Performing Test CMAKE_HAVE_
- [valkey-io/valkey#3800](https://github.com/valkey-io/valkey/pull/3800): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#3498](https://github.com/valkey-io/valkey/pull/3498): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
