# Fuzzer walk

Updated 2026-11-08T08:09+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 887 | 1620 | #2809 (2025-12-03) | 321 | 126.7 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 570 | 99 | #131 (2025-05-19) | 177 | 81.4 |

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

[2026-11-08T08:09:25+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42461667303): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-08T07:57:56+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42460867633): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-08T06:10:26+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42447288100): 20 merge(s) tested, 2 without a code change.

- [valkey-io/valkey-search#172](https://github.com/valkey-io/valkey-search/pull/172): to retry, `export NINJA_TOOL=ninja` failed in parent (exit 1): [20/206] Building CXX object vmsdk/src/CMakeFiles/memory_allocation_overrides.dir/memory_allocation_overrides.cc.o [21/206] Linking CXX static libr
- [valkey-io/valkey-search#144](https://github.com/valkey-io/valkey-search/pull/144): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): [23/206] Building C object third_party/hdrhistogram_c/src/CMakeFiles/hdrhistogram_c.dir/hdr_interval_recorder.c.o [24/206] Building C object third_p
- [valkey-io/valkey-search#168](https://github.com/valkey-io/valkey-search/pull/168): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): [14/206] Linking CXX static library src/indexes/libindex_schema_cc_proto.a [15/206] Linking CXX static library vmsdk/src/libmemory_allocation.a [16/
- [valkey-io/valkey-search#166](https://github.com/valkey-io/valkey-search/pull/166): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): [9/206] Building CXX object vmsdk/src/CMakeFiles/log.dir/log.cc.o [10/206] Running cpp protocol buffer compiler on /home/runner/work/valkey-ci-agent
- [valkey-io/valkey-search#162](https://github.com/valkey-io/valkey-search/pull/162): to retry, `export NINJA_TOOL=ninja` failed in parent (exit 1): [20/206] Linking CXX static library vmsdk/src/libmemory_allocation_overrides.a [21/206] Building C object third_party/hdrhistogram_c/src/CMakeFiles
- [valkey-io/valkey-search#161](https://github.com/valkey-io/valkey-search/pull/161): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): [13/206] Linking CXX static library vmsdk/src/libmemory_allocation.a [14/206] Building CXX object src/indexes/CMakeFiles/index_schema_cc_proto.dir/_
- [valkey-io/valkey-search#159](https://github.com/valkey-io/valkey-search/pull/159): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): [18/206] Building CXX object vmsdk/src/CMakeFiles/memory_allocation_overrides.dir/memory_allocation_overrides.cc.o [19/206] Building CXX object thir
- [valkey-io/valkey-search#158](https://github.com/valkey-io/valkey-search/pull/158): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): [8/206] Building CXX object src/indexes/CMakeFiles/index_schema_cc_proto.dir/__/index_schema.pb.cc.o [9/206] Building CXX object vmsdk/src/CMakeFile
- [valkey-io/valkey-search#148](https://github.com/valkey-io/valkey-search/pull/148): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): [8/206] Building CXX object src/indexes/CMakeFiles/index_schema_cc_proto.dir/__/index_schema.pb.cc.o [9/206] Building CXX object vmsdk/src/CMakeFile
- [valkey-io/valkey-search#154](https://github.com/valkey-io/valkey-search/pull/154): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): [21/206] Building CXX object src/CMakeFiles/attribute_data_type.dir/attribute_data_type.cc.o [22/206] Building C object third_party/hdrhistogram_c/s
- [valkey-io/valkey-search#152](https://github.com/valkey-io/valkey-search/pull/152): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): [13/206] Linking CXX static library vmsdk/src/libmemory_allocation.a [14/206] Building CXX object src/indexes/CMakeFiles/index_schema_cc_proto.dir/_
- [valkey-io/valkey-search#149](https://github.com/valkey-io/valkey-search/pull/149): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): [13/206] Linking CXX static library vmsdk/src/libmemory_allocation.a [14/206] Building CXX object src/indexes/CMakeFiles/index_schema_cc_proto.dir/_
- [valkey-io/valkey-search#146](https://github.com/valkey-io/valkey-search/pull/146): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): [20/206] Linking CXX static library src/libattribute_data_type.a [21/206] Linking CXX static library vmsdk/src/libmemory_allocation_overrides.a [22/
- [valkey-io/valkey-search#129](https://github.com/valkey-io/valkey-search/pull/129): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): [7/206] Building CXX object vmsdk/src/CMakeFiles/memory_allocation.dir/memory_allocation.cc.o [8/206] Building CXX object src/indexes/CMakeFiles/ind
- [valkey-io/valkey-search#143](https://github.com/valkey-io/valkey-search/pull/143): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): [7/202] Building CXX object vmsdk/src/CMakeFiles/memory_allocation.dir/memory_allocation.cc.o [8/202] Building CXX object src/indexes/CMakeFiles/ind
- [valkey-io/valkey-search#138](https://github.com/valkey-io/valkey-search/pull/138) ([tests](tests/valkey-search/138-41c4ff082/)): Fixing empty vector query clause ft.search cmd parsing (#138): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#137](https://github.com/valkey-io/valkey-search/pull/137) ([tests](tests/valkey-search/137-5001de38f/)): renaming total_indexed_hash_keys to total_indexed_documents (#137): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#135](https://github.com/valkey-io/valkey-search/pull/135) ([tests](tests/valkey-search/135-3e9874028/)): Add OOM checking to index backfill (#135): 3 of 3 test(s) reached the change; crash, predates #135, since fixed (recorded)
- [valkey-io/valkey-search#136](https://github.com/valkey-io/valkey-search/pull/136) ([tests](tests/valkey-search/136-6fab2d034/)): Remove Google-internal specifics and unused code (#136): declined: The change is a pure cleanup with no behaviour change: it removes Google-internal comment references (a screenshot link and Buganizer bug numbers in TODOs), deletes dead declarations (SchemaManager::OnSavingStarted/OnSavingEnded have no definitions, callers, or server-event subscriptions a
- [valkey-io/valkey-search#125](https://github.com/valkey-io/valkey-search/pull/125) ([tests](tests/valkey-search/125-12ec716c1/)): Command registration permissions alignment with ACL prefix permissions (#125): 4 of 4 test(s) reached the change

## Totals

- Merges whose tests reached the change: 565 of 823 tested (69%)
- Changed-line coverage: median 100% of 408 merge(s) measured, 30 under 50%
- Recall: 159 parents flagged of 233 fix merges
- Claude: $1797.66 in the last 24 hours, $5911.22 in the last 7 days, $5911.22 in all
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

- [valkey-io/valkey-search#172](https://github.com/valkey-io/valkey-search/pull/172): attempt 2 of 3, `export NINJA_TOOL=ninja` failed in parent (exit 1): [20/206] Building CXX object vmsdk/src/CMakeFiles/memory_allocation_overrides.dir/memory_allocation_overrides.cc.o [21/206] Linking CXX static libr
- [valkey-io/valkey-search#144](https://github.com/valkey-io/valkey-search/pull/144): attempt 2 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): [23/206] Building C object third_party/hdrhistogram_c/src/CMakeFiles/hdrhistogram_c.dir/hdr_interval_recorder.c.o [24/206] Building C object third_p
- [valkey-io/valkey-search#168](https://github.com/valkey-io/valkey-search/pull/168): attempt 2 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): [14/206] Linking CXX static library src/indexes/libindex_schema_cc_proto.a [15/206] Linking CXX static library vmsdk/src/libmemory_allocation.a [16/
- [valkey-io/valkey-search#166](https://github.com/valkey-io/valkey-search/pull/166): attempt 2 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): [9/206] Building CXX object vmsdk/src/CMakeFiles/log.dir/log.cc.o [10/206] Running cpp protocol buffer compiler on /home/runner/work/valkey-ci-agent
- [valkey-io/valkey-search#162](https://github.com/valkey-io/valkey-search/pull/162): attempt 2 of 3, `export NINJA_TOOL=ninja` failed in parent (exit 1): [20/206] Linking CXX static library vmsdk/src/libmemory_allocation_overrides.a [21/206] Building C object third_party/hdrhistogram_c/src/CMakeFiles
- [valkey-io/valkey-search#161](https://github.com/valkey-io/valkey-search/pull/161): attempt 2 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): [13/206] Linking CXX static library vmsdk/src/libmemory_allocation.a [14/206] Building CXX object src/indexes/CMakeFiles/index_schema_cc_proto.dir/_
- [valkey-io/valkey-search#159](https://github.com/valkey-io/valkey-search/pull/159): attempt 2 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): [18/206] Building CXX object vmsdk/src/CMakeFiles/memory_allocation_overrides.dir/memory_allocation_overrides.cc.o [19/206] Building CXX object thir
- [valkey-io/valkey-search#158](https://github.com/valkey-io/valkey-search/pull/158): attempt 2 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): [8/206] Building CXX object src/indexes/CMakeFiles/index_schema_cc_proto.dir/__/index_schema.pb.cc.o [9/206] Building CXX object vmsdk/src/CMakeFile
- [valkey-io/valkey-search#148](https://github.com/valkey-io/valkey-search/pull/148): attempt 2 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): [8/206] Building CXX object src/indexes/CMakeFiles/index_schema_cc_proto.dir/__/index_schema.pb.cc.o [9/206] Building CXX object vmsdk/src/CMakeFile
- [valkey-io/valkey-search#154](https://github.com/valkey-io/valkey-search/pull/154): attempt 2 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): [21/206] Building CXX object src/CMakeFiles/attribute_data_type.dir/attribute_data_type.cc.o [22/206] Building C object third_party/hdrhistogram_c/s
- [valkey-io/valkey-search#152](https://github.com/valkey-io/valkey-search/pull/152): attempt 1 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): [13/206] Linking CXX static library vmsdk/src/libmemory_allocation.a [14/206] Building CXX object src/indexes/CMakeFiles/index_schema_cc_proto.dir/_
- [valkey-io/valkey-search#149](https://github.com/valkey-io/valkey-search/pull/149): attempt 1 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): [13/206] Linking CXX static library vmsdk/src/libmemory_allocation.a [14/206] Building CXX object src/indexes/CMakeFiles/index_schema_cc_proto.dir/_
- [valkey-io/valkey-search#146](https://github.com/valkey-io/valkey-search/pull/146): attempt 1 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): [20/206] Linking CXX static library src/libattribute_data_type.a [21/206] Linking CXX static library vmsdk/src/libmemory_allocation_overrides.a [22/
- [valkey-io/valkey-search#129](https://github.com/valkey-io/valkey-search/pull/129): attempt 1 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): [7/206] Building CXX object vmsdk/src/CMakeFiles/memory_allocation.dir/memory_allocation.cc.o [8/206] Building CXX object src/indexes/CMakeFiles/ind
- [valkey-io/valkey-search#143](https://github.com/valkey-io/valkey-search/pull/143): attempt 1 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): [7/202] Building CXX object vmsdk/src/CMakeFiles/memory_allocation.dir/memory_allocation.cc.o [8/202] Building CXX object src/indexes/CMakeFiles/ind
- [valkey-io/valkey#3800](https://github.com/valkey-io/valkey/pull/3800): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#3613](https://github.com/valkey-io/valkey/pull/3613): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#3498](https://github.com/valkey-io/valkey/pull/3498): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
