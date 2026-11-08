# Fuzzer walk

Updated 2026-11-08T09:58+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 887 | 1620 | #2809 (2025-12-03) | 298 | 126.7 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 605 | 64 | #82 (2025-03-31) | 212 | 86.4 |

## Last runs

### valkey-io/valkey

[2026-11-08T08:08:13+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42450654882): 20 merge(s) tested, 6 without a code change.

- [valkey-io/valkey#3800](https://github.com/valkey-io/valkey/pull/3800): to retry, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#1032](https://github.com/valkey-io/valkey/pull/1032) ([tests](tests/valkey/1032-82d7b7e40/)): Fail to save the cluster config file will not exit the process (#1032): 0 of 3 test(s) reached the change, coverage 85%; [crash, predates #1032, which hid it](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/103)
- [valkey-io/valkey#2943](https://github.com/valkey-io/valkey/pull/2943) ([tests](tests/valkey/2943-495650676/)): Enforce 64‑bit off_t regardless of include order; prevent LTO type mismatch (#2943): declined: The change is purely compile-time: it adds fmacros.h includes to headers and unit-test files and a static_assert(sizeof(off_t) >= 8) in server.h, so that _FILE_OFFSET_BITS=64 is in effect before system headers and any include-order regression fails the build rather than producing a 32-bit 
- [valkey-io/valkey#2959](https://github.com/valkey-io/valkey/pull/2959) ([tests](tests/valkey/2959-b7a7ef8eb/)): Propagate database changes from scripting engine temp client (#2959): 4 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2932](https://github.com/valkey-io/valkey/pull/2932) ([tests](tests/valkey/2932-a571ac527/)): Fix memory allocation size in getClusterNodesList (#2932): declined: The change shrinks an over-allocation in getClusterNodesList from (count+1)*CLUSTER_NAMELEN (40 bytes/element) to the correct (count+1)*sizeof(char *) (8 bytes/element). The old size was always larger than needed, so there was never an out-of-bounds access, crash, or data risk — only waste
- [valkey-io/valkey#2955](https://github.com/valkey-io/valkey/pull/2955) ([tests](tests/valkey/2955-0a9cb985c/)): Fix lua module build failure when using CLANG (#2955): declined: The change only edits src/modules/lua/Makefile: it detects clang, switches -std=c99 to -std=gnu11, and adds -fuse-ld=lld for clang linking. It is a build-system fix with no runtime behaviour change — no command, reply, replication, persistence or cluster path is affected, so there is nothi
- [valkey-io/valkey#2472](https://github.com/valkey-io/valkey/pull/2472) ([tests](tests/valkey/2472-e9014fd02/)): Adding support for sharing memory between the module and the engine (#2472): 0 of 11 test(s) reached the change, coverage 58%
- [valkey-io/valkey#2950](https://github.com/valkey-io/valkey/pull/2950) ([tests](tests/valkey/2950-512ffdb80/)): Module API docs formatting and improve script (#2950): declined: The change edits only comments in src/module.c (a function-name typo and comment line wrapping in a doc example) and the offline Ruby documentation-generation script utils/generate-module-api-doc.rb. No executable server code, command behaviour, reply format, replication, persistence, or m
- [valkey-io/valkey#2858](https://github.com/valkey-io/valkey/pull/2858) ([tests](tests/valkey/2858-29bf862bc/)): Move Lua scripting engine into a Valkey module (#2858): 3 of 13 test(s) reached the change, coverage 81%
- [valkey-io/valkey#2735](https://github.com/valkey-io/valkey/pull/2735) ([tests](tests/valkey/2735-d22fcfb03/)): Respect REPLCONF VERSION in diskless full sync (#2735): 0 of 17 test(s) reached the change, coverage 43%, 2 skipped (required capability 'build' is unsupported: default: build rejects startup configuration: *** FATAL CONFIG FILE ERROR (Version 255.255.255) *** Reading the configuration file, at line 11 >>> 'forkless-infrastructure-enabled "yes"' Bad directive or wrong number of arguments; default deployment config)
- [valkey-io/valkey#2852](https://github.com/valkey-io/valkey/pull/2852) ([tests](tests/valkey/2852-8ab0152be/)): Check for duplicate nodeids when loading nodes.conf (#2852): 0 of 4 test(s) reached the change, coverage 0%, 4 skipped (required capability 'save-behavior-support' is unsupported: default: build rejects startup configuration: *** FATAL CONFIG FILE ERROR (Version 255.255.255) *** Reading the configuration file, at line 11 >>> 'cluster-config-save-behavior "sync"' Bad directive or wrong number of arguments)
- [valkey-io/valkey#2872](https://github.com/valkey-io/valkey/pull/2872) ([tests](tests/valkey/2872-51f871ae5/)): Strictly check CRLF when parsing querybuf (#2872): 0 of 5 test(s) reached the change, coverage 0%
- [valkey-io/valkey#2925](https://github.com/valkey-io/valkey/pull/2925) ([tests](tests/valkey/2925-5940dbfb0/)): Revert "Allow partial sync after loading AOF with preamble (#2366)" (#2925): 2 of 9 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2915](https://github.com/valkey-io/valkey/pull/2915) ([tests](tests/valkey/2915-2a9a1731e/)): Fix CLUSTER SLOTS crash when called from module timer callback (#2915): declined: no proposed test validated
- [valkey-io/valkey#2652](https://github.com/valkey-io/valkey/pull/2652) ([tests](tests/valkey/2652-48e0cbbb4/)): Fix commandlog large-reply when using reply copy avoidance (#2652): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2866](https://github.com/valkey-io/valkey/pull/2866) ([tests](tests/valkey/2866-29d324493/)): Make the COB soft limit also use repl-backlog-size when its value is smaller (#2866): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2876](https://github.com/valkey-io/valkey/pull/2876) ([tests](tests/valkey/2876-04d0bba39/)): support whole cluster info for INFO command in cluster section (#2876): 2 of 4 test(s) reached the change, coverage 75%
- [valkey-io/valkey#2898](https://github.com/valkey-io/valkey/pull/2898) ([tests](tests/valkey/2898-3d65a4aec/)): Fix deadlock in IO thread shutdown during panic (#2898): 0 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2807](https://github.com/valkey-io/valkey/pull/2807) ([tests](tests/valkey/2807-70196ee20/)): Add safe iterator tracking in hashtable to prevents invalid access on hashtable delete (#2807): 0 of 11 test(s) reached the change, coverage 41%, 3 skipped (required capability 'build' is unsupported: default: build rejects startup configuration: *** FATAL CONFIG FILE ERROR (Version 255.255.255) *** Reading the configuration file, at line 11 >>> 'forkless-infrastructure-enabled "yes"' Bad directive or wrong number of arguments; default deployment config)
- [valkey-io/valkey#2840](https://github.com/valkey-io/valkey/pull/2840) ([tests](tests/valkey/2840-8ec938197/)): Send duplicate multi meet packet only for node which supports it (#2840): 0 of 3 test(s) reached the change, coverage 80%

### valkey-io/valkey-bloom

[2026-11-08T09:41:25+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42468200910): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-08T09:31:27+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42467488104): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-08T09:58:06+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42462123332): 20 merge(s) tested, 2 without a code change.

- [valkey-io/valkey-search#152](https://github.com/valkey-io/valkey-search/pull/152): More resilient builds on different distros (#152): error: `export NINJA_TOOL=ninja` failed in merge (exit 2): [23/206] Building CXX object vmsdk/src/CMakeFiles/module.dir/module.cc.o [24/206] Building C object third_party/hdrhistogram_c/src/CMakeFiles/hdrhistogram_c.dir/hdr_histogram.c.o [25/206] Building C object third_party/hdrhistogram_c/src/CMak
- [valkey-io/valkey-search#149](https://github.com/valkey-io/valkey-search/pull/149): Module-Config: fix a possibility of of a double free (#149): error: `export NINJA_TOOL=ninja` failed in merge (exit 2): [9/206] Linking CXX static library vmsdk/src/libutils.a [10/206] Linking CXX static library src/utils/liballocator.a [11/206] Linking CXX static library vmsdk/src/status/libstatus_macros.a [12/206] Linking CXX static library src/indexes/libi
- [valkey-io/valkey-search#146](https://github.com/valkey-io/valkey-search/pull/146): Dropped the deprecated `--threads` flag (#146): error: `export NINJA_TOOL=ninja` failed in merge (exit 2): [22/206] Linking CXX static library vmsdk/src/libmemory_allocation_overrides.a [23/206] Building C object third_party/hdrhistogram_c/src/CMakeFiles/hdrhistogram_c.dir/hdr_encoding.c.o [24/206] Building C object third_party/hdrhistogram_c/src
- [valkey-io/valkey-search#129](https://github.com/valkey-io/valkey-search/pull/129): Allow changing the reader/writer threads count at runtime (#129): error: `export NINJA_TOOL=ninja` failed in merge (exit 2): [8/206] Building CXX object src/indexes/CMakeFiles/index_schema_cc_proto.dir/__/index_schema.pb.cc.o [9/206] Building CXX object vmsdk/src/CMakeFiles/log.dir/log.cc.o [10/206] Running cpp protocol buffer compiler on /home/runner/work/valkey-
- [valkey-io/valkey-search#143](https://github.com/valkey-io/valkey-search/pull/143): Use background context rather than IO context during Aux save. (#143): error: `export NINJA_TOOL=ninja` failed in merge (exit 2): [23/202] Building C object third_party/hdrhistogram_c/src/CMakeFiles/hdrhistogram_c.dir/hdr_thread.c.o [24/202] Building C object third_party/hdrhistogram_c/src/CMakeFiles/hdrhistogram_c.dir/hdr_time.c.o [25/202] Building C object third_part
- [valkey-io/valkey-search#119](https://github.com/valkey-io/valkey-search/pull/119) ([tests](tests/valkey-search/119-4e3fa7f2b/)): Rename ValkeySearch to valkey-search (#119): declined: The change is purely cosmetic: it renames 'ValkeySearch' to 'valkey-search' in copyright headers, LICENSE, QUICK_START.md and the CMake project() name, plus a few clang-format whitespace fixes (ternary spacing in vector_base.cc, brace spacing in tag.h, indentation in status_builder.h and v
- [valkey-io/valkey-search#105](https://github.com/valkey-io/valkey-search/pull/105) ([tests](tests/valkey-search/105-eff3e03bf/)): chore: avoid reserved gid, group name when setup for local dev env (#105): declined: The change modifies only the local development environment setup (.devcontainer/Dockerfile group-GID conflict handling and a setup.sh echo typo). It is a build/dev-tooling change with no effect on the valkey-search module's runtime behavior, commands, replies, or data handling, so no scena
- [valkey-io/valkey-search#109](https://github.com/valkey-io/valkey-search/pull/109) ([tests](tests/valkey-search/109-2cd8b1fb2/)): Exposing search subscription info just if built with the macro DEBUG_INFO (#109): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#107](https://github.com/valkey-io/valkey-search/pull/107) ([tests](tests/valkey-search/107-0a803337b/)): Support counting cores on aarch64 (#107): declined: no proposed test validated
- [valkey-io/valkey-search#106](https://github.com/valkey-io/valkey-search/pull/106) ([tests](tests/valkey-search/106-356ece8c6/)): Fixed crash in UT (#106): declined: The change modifies only unit-test infrastructure: cmake/Modules/valkey_search.cmake drops a strip linker flag for test targets, and vmsdk/src/testing_infra/module.h adds a mock for RedisModule_RegisterNumericConfig used by googletest-based unit tests. Neither is compiled into the shipped 
- [valkey-io/valkey-search#97](https://github.com/valkey-io/valkey-search/pull/97) ([tests](tests/valkey-search/97-5e50c97e5/)): Make backfill batch/block size configurable (#97): 2 of 2 test(s) reached the change; crash, predates #97, since fixed (recorded)
- [valkey-io/valkey-search#104](https://github.com/valkey-io/valkey-search/pull/104) ([tests](tests/valkey-search/104-8a391acfc/)): FT.SEARCH - Update neighbor sorting to always be asc (#104): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#101](https://github.com/valkey-io/valkey-search/pull/101) ([tests](tests/valkey-search/101-a87755dd7/)): Fixing asan issues (#101): 0 of 5 test(s) reached the change; crash, predates #101, since fixed (recorded)
- [valkey-io/valkey-search#95](https://github.com/valkey-io/valkey-search/pull/95) ([tests](tests/valkey-search/95-1e30f7114/)): adding support for blocking clients during keyspace notification (#95): 0 of 5 test(s) reached the change; [crash, appeared after #95](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/87); crash, introduced and since fixed (recorded); crash, predates #95, since fixed (recorded)
- [valkey-io/valkey-search#98](https://github.com/valkey-io/valkey-search/pull/98) ([tests](tests/valkey-search/98-3a3676748/)): avoid unsafe code in ValkeySearch::Info  during crashdump (#98): declined: The change only affects ValkeySearch::Info when invoked with for_crash_report set, i.e. from inside Valkey's crash-dump signal handler: it skips module INFO sections that take locks, allocate, or need the main thread. The normal INFO path (for_crash_report=0) emits exactly the same section
- [valkey-io/valkey-search#102](https://github.com/valkey-io/valkey-search/pull/102) ([tests](tests/valkey-search/102-94ec3e5fb/)): More ASan build improvements (#102): declined: The change modifies only build tooling: build.sh (clearer output messages and continuing past unit-test failures in ASan runs), a CMake refactor extracting the unchanged ASan compile flags into a helper function, and applying those flags to the test-only testing_common static library. No r
- [valkey-io/valkey-search#100](https://github.com/valkey-io/valkey-search/pull/100) ([tests](tests/valkey-search/100-aa37bda43/)): Add ASAN build option (#100): declined: The change only adds an opt-in ASAN build option: a build.sh flag, CMake options that add -fsanitize=address, and #ifndef ASAN_BUILD guards around the vmsdk operator new/delete overrides. ASAN_BUILD is OFF by default and the runner builds the module without it, so the compiled module at th
- [valkey-io/valkey-search#89](https://github.com/valkey-io/valkey-search/pull/89) ([tests](tests/valkey-search/89-15e26b65e/)): Don't suppress error when running python integration tests. (#89): declined: The change modifies only testing/integration/CMakeLists.txt, removing OUTPUT_QUIET/ERROR_QUIET from execute_process calls so pip/venv setup errors are no longer suppressed during CMake configuration of the python integration tests (plus a trailing-newline fix). It is a build/CI change with
- [valkey-io/valkey-search#88](https://github.com/valkey-io/valkey-search/pull/88): to retry, `sudo apt-get install -y cmake ninja-build` failed in walk-work (exit 22): Preparing to unpack .../librhash1_1.4.6-1.1_amd64v3.deb ... Unpacking librhash1:amd64 (1.4.6-1.1) ... Selecting previously un
- [valkey-io/valkey-search#85](https://github.com/valkey-io/valkey-search/pull/85): to retry, `export NINJA_TOOL=ninja` failed in parent (exit 2): -- Detecting CXX compile features -- Detecting CXX compile features - done -- Performing Test CMAKE_HAVE_LIBC_PTHREAD -- Performing Test CMAKE_HAVE

## Totals

- Merges whose tests reached the change: 569 of 830 tested (69%)
- Changed-line coverage: median 100% of 408 merge(s) measured, 30 under 50%
- Recall: 159 parents flagged of 234 fix merges
- Claude: $1800.18 in the last 24 hours, $5983.68 in the last 7 days, $5983.68 in all
- Issues filed: 54
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/100
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/101
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/102
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/103
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

- [valkey-io/valkey-search#88](https://github.com/valkey-io/valkey-search/pull/88): attempt 1 of 3, `sudo apt-get install -y cmake ninja-build` failed in walk-work (exit 22): Preparing to unpack .../librhash1_1.4.6-1.1_amd64v3.deb ... Unpacking librhash1:amd64 (1.4.6-1.1) ... Selecting previously un
- [valkey-io/valkey-search#85](https://github.com/valkey-io/valkey-search/pull/85): attempt 1 of 3, `export NINJA_TOOL=ninja` failed in parent (exit 2): -- Detecting CXX compile features -- Detecting CXX compile features - done -- Performing Test CMAKE_HAVE_LIBC_PTHREAD -- Performing Test CMAKE_HAVE
- [valkey-io/valkey#3800](https://github.com/valkey-io/valkey/pull/3800): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#3613](https://github.com/valkey-io/valkey/pull/3613): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#3498](https://github.com/valkey-io/valkey/pull/3498): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
