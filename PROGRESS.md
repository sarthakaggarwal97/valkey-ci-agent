# Fuzzer walk

Updated 2026-11-08T06:37+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 862 | 1645 | #1032 (2025-12-22) | 296 | 123.1 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 570 | 99 | #131 (2025-05-19) | 198 | 81.4 |

## Last runs

### valkey-io/valkey

[2026-11-08T05:24:21+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42436474417): 20 merge(s) tested, 2 without a code change.

- [valkey-io/valkey#2964](https://github.com/valkey-io/valkey/pull/2964) ([tests](tests/valkey/2964-82a312d8a/)): replace info cluster info in a dedicated new section (#2964): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3007](https://github.com/valkey-io/valkey/pull/3007) ([tests](tests/valkey/3007-dbe07b7c9/)): Untrack key based on old->hasembkey (#3007): 2 of 4 test(s) reached the change, coverage 100%; crash, fixed by #3007 (recorded)
- [valkey-io/valkey#3003](https://github.com/valkey-io/valkey/pull/3003) ([tests](tests/valkey/3003-e4a3e9f0a/)): fix(hash): Untrack hash with volatile fields when it is overwritten  (#3003): 2 of 2 test(s) reached the change, coverage 100%; crash, fixed by #3003 (recorded)
- [valkey-io/valkey#2975](https://github.com/valkey-io/valkey/pull/2975) ([tests](tests/valkey/2975-fd56b21df/)): Add sdsnsplitargs to reduce sds allocation. (#2975): 0 of 8 test(s) reached the change, coverage 90%
- [valkey-io/valkey#2998](https://github.com/valkey-io/valkey/pull/2998) ([tests](tests/valkey/2998-263d9ea41/)): Fix zero length hash creation with HSETEX (#2998): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2985](https://github.com/valkey-io/valkey/pull/2985) ([tests](tests/valkey/2985-d10b06009/)): Avoid useless seek in xtrim (#2985): declined: The change adds an early return in streamTrim when trim_strategy is MAXLEN and s->length <= maxlen, purely to skip allocating and seeking a rax iterator. The identical condition was already the first check inside the trim loop (t_stream.c:726), so the old code broke out of the loop on the 
- [valkey-io/valkey#3000](https://github.com/valkey-io/valkey/pull/3000) ([tests](tests/valkey/3000-9a8f1c8c1/)): fix(HSETEX): replace strcmp with strcasecmp (#3000): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2425](https://github.com/valkey-io/valkey/pull/2425) ([tests](tests/valkey/2425-50667e0ad/)): Skip sorting empty lists in sortCommandGeneric to prevent undefined behavior when calling pqsort (#2425): 0 of 6 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2895](https://github.com/valkey-io/valkey/pull/2895) ([tests](tests/valkey/2895-15e79ef00/)): Add FIFO and mutexQueue with bio.c refactored (#2895): 0 of 18 test(s) reached the change, coverage 48%
- [valkey-io/valkey#2340](https://github.com/valkey-io/valkey/pull/2340) ([tests](tests/valkey/2340-e843c18f1/)): Valkey Fuzzer (#2340): 7 of 7 test(s) reached the change, coverage 86%
- [valkey-io/valkey#2973](https://github.com/valkey-io/valkey/pull/2973) ([tests](tests/valkey/2973-b56543285/)): avoid memory leak of new argv when hexpire commands target only non-exiting fields (#2973): 0 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2974](https://github.com/valkey-io/valkey/pull/2974) ([tests](tests/valkey/2974-7cef1e8ee/)): fix hincrby* update volatile key tracking (#2974): 2 of 2 test(s) reached the change, coverage 100%; check, introduced and since fixed (recorded)
- [valkey-io/valkey#2983](https://github.com/valkey-io/valkey/pull/2983) ([tests](tests/valkey/2983-9c5d004bf/)): Fix chained replica crash when doing dual channel replication (#2983): 0 of 6 test(s) reached the change, coverage 0%
- [valkey-io/valkey#2981](https://github.com/valkey-io/valkey/pull/2981) ([tests](tests/valkey/2981-e179fddcf/)): Revert hgetall dynamic deferred response  (#2981): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2963](https://github.com/valkey-io/valkey/pull/2963) ([tests](tests/valkey/2963-2a6137be0/)): Fix compile overflow warnings (#2963): declined: The only change adds a defensive guard (`sdslen(parts[0]) < 2`) in evalExtractShebangFlags to silence a compiler integer-underflow warning on `sdslen(parts[0]) - 2`. The function body only runs when the script starts with the literal `#!`, and the first token from sdssplitargs always begin
- [valkey-io/valkey#2944](https://github.com/valkey-io/valkey/pull/2944) ([tests](tests/valkey/2944-26162effd/)): Restrict ttl from being negative and avoid crash in import-mode (#2944): 2 of 2 test(s) reached the change, coverage 100%; crash, fixed by #2944 (recorded)
- [valkey-io/valkey#2966](https://github.com/valkey-io/valkey/pull/2966) ([tests](tests/valkey/2966-75707ac43/)): Avoid hgetall deferred response (#2966): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2309](https://github.com/valkey-io/valkey/pull/2309) ([tests](tests/valkey/2309-122070cf8/)): Database-level access control (#2309): 7 of 7 test(s) reached the change, coverage not measured ([Errno 2] No such file or directory: 'walk-work/coverage/src/cli_commands.c.gcov')
- [valkey-io/valkey#2755](https://github.com/valkey-io/valkey/pull/2755) ([tests](tests/valkey/2755-328b3ce3c/)): Add support for Atomic Slot Migration to CLI (#2755): 5 of 5 test(s) reached the change, coverage 72%
- [valkey-io/valkey#2935](https://github.com/valkey-io/valkey/pull/2935) ([tests](tests/valkey/2935-992b88626/)): Fix incorrect comment about STATS_METRIC_* Macro in server.h (#2935): declined: The change only edits two comments in server.h describing the STATS_METRIC_NET_INPUT and STATS_METRIC_NET_INPUT_REPLICATION enum values ('read to network' → 'read from network'). No code, behaviour, reply, or binary output changes, so there is nothing to test.

### valkey-io/valkey-bloom

[2026-11-08T06:37:17+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42455460952): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-08T06:26:04+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42454690477): 0 merge(s) tested, 0 without a code change.


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

- Merges whose tests reached the change: 560 of 809 tested (69%)
- Changed-line coverage: median 100% of 394 merge(s) measured, 26 under 50%
- Recall: 158 parents flagged of 231 fix merges
- Claude: $1752.32 in the last 24 hours, $5766.66 in the last 7 days, $5766.66 in all
- Issues filed: 53
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/100
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/101
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/102
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
